package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"get.porter.sh/porter/pkg/cnab"
	"get.porter.sh/porter/pkg/portercontext"
	"get.porter.sh/porter/pkg/storage/plugins"
	"get.porter.sh/porter/pkg/tracing"
	"go.mongodb.org/mongo-driver/bson"
	_ "modernc.org/sqlite"
)

var (
	_ plugins.StorageProtocol = &Store{}

	errUnsupportedAggregate = errors.New("sqlite storage only supports the last outputs aggregate pipeline")
	errUnsupportedPatch     = errors.New("sqlite storage only supports $set and $unset patch operations")
	validTableName          = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
)

// Store implements the Porter plugins.StorageProtocol interface for sqlite.
type Store struct {
	*portercontext.Context
	path    string
	timeout time.Duration
	db      *sql.DB
}

// NewStore creates a new storage engine that uses sqlite.
func NewStore(c *portercontext.Context, cfg PluginConfig) *Store {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 // default to 10 seconds
	}
	return &Store{
		Context: c,
		path:    cfg.Path,
		timeout: time.Duration(timeout) * time.Second,
	}
}

// Connect initializes the plugin for use.
// Close is called automatically when the plugin is used by Porter.
func (s *Store) Connect(ctx context.Context) error {
	if s.db != nil {
		return nil
	}

	ctx, span := tracing.StartSpan(ctx)
	defer span.EndSpan()

	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return span.Error(fmt.Errorf("could not create sqlite directory: %w", err))
	}

	db, err := sql.Open("sqlite", s.path)
	if err != nil {
		return span.Error(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	cxt, cancel := s.withTimeout(ctx)
	defer cancel()
	if _, err := db.ExecContext(cxt, "PRAGMA busy_timeout = 5000"); err != nil {
		_ = db.Close()
		return span.Error(err)
	}

	s.db = db
	return nil
}

func (s *Store) Close() error {
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			return err
		}
		s.db = nil
	}
	return nil
}

func (s *Store) EnsureIndex(ctx context.Context, opts plugins.EnsureIndexOptions) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.EndSpan()
	if err := s.Connect(ctx); err != nil {
		return err
	}

	for _, index := range opts.Indices {
		table, err := s.ensureCollection(ctx, index.Collection)
		if err != nil {
			return span.Error(err)
		}

		indexName, columns, err := s.indexDefinition(table, index.Keys)
		if err != nil {
			return span.Error(err)
		}
		if len(columns) == 0 {
			continue
		}

		unique := ""
		if index.Unique {
			unique = "UNIQUE "
		}
		query := fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)", unique, indexName, table, strings.Join(columns, ", "))
		cxt, cancel := s.withTimeout(ctx)
		_, err = s.db.ExecContext(cxt, query)
		cancel()
		if err != nil {
			return span.Error(err)
		}
	}

	return nil
}

func (s *Store) Aggregate(ctx context.Context, opts plugins.AggregateOptions) ([]bson.Raw, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.EndSpan()
	if err := s.Connect(ctx); err != nil {
		return nil, err
	}

	match, ok := parseLastOutputsAggregate(opts.Pipeline)
	if !ok {
		return nil, span.Error(errUnsupportedAggregate)
	}

	table, err := s.ensureCollection(ctx, opts.Collection)
	if err != nil {
		return nil, span.Error(err)
	}

	whereClause, args, err := s.buildWhereClause(match)
	if err != nil {
		return nil, span.Error(err)
	}

	query := fmt.Sprintf(`
SELECT doc, name FROM (
	SELECT doc, name,
		ROW_NUMBER() OVER (PARTITION BY name ORDER BY resultId DESC) AS rn
	FROM %s%s
) WHERE rn = 1`, table, whereClause)

	cxt, cancel := s.withTimeout(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(cxt, query, args...)
	if err != nil {
		return nil, span.Error(err)
	}
	defer rows.Close()

	var results []bson.Raw
	for rows.Next() {
		var docBytes []byte
		var name sql.NullString
		if err := rows.Scan(&docBytes, &name); err != nil {
			return nil, span.Error(err)
		}

		doc, err := unmarshalDocument(docBytes)
		if err != nil {
			return nil, span.Error(err)
		}

		grouped := bson.M{
			"_id":        name.String,
			"lastOutput": doc,
		}
		raw, err := bson.Marshal(grouped)
		if err != nil {
			return nil, span.Error(err)
		}
		results = append(results, raw)
	}

	return results, span.Error(rows.Err())
}

func (s *Store) Count(ctx context.Context, opts plugins.CountOptions) (int64, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.EndSpan()
	if err := s.Connect(ctx); err != nil {
		return 0, err
	}

	table, err := s.ensureCollection(ctx, opts.Collection)
	if err != nil {
		return 0, span.Error(err)
	}

	whereClause, args, err := s.buildWhereClause(opts.Filter)
	if err != nil {
		return 0, span.Error(err)
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s%s", table, whereClause)
	cxt, cancel := s.withTimeout(ctx)
	defer cancel()
	row := s.db.QueryRowContext(cxt, query, args...)

	var count int64
	if err := row.Scan(&count); err != nil {
		return 0, span.Error(err)
	}
	return count, nil
}

func (s *Store) Find(ctx context.Context, opts plugins.FindOptions) ([]bson.Raw, error) {
	ctx, span := tracing.StartSpan(ctx)
	defer span.EndSpan()
	if err := s.Connect(ctx); err != nil {
		return nil, err
	}

	table, err := s.ensureCollection(ctx, opts.Collection)
	if err != nil {
		return nil, span.Error(err)
	}

	whereClause, args, err := s.buildWhereClause(opts.Filter)
	if err != nil {
		return nil, span.Error(err)
	}

	orderBy, err := s.buildOrderBy(opts.Sort)
	if err != nil {
		return nil, span.Error(err)
	}

	limitClause, limitArgs := buildLimitClause(opts.Limit, opts.Skip)
	query := fmt.Sprintf("SELECT doc FROM %s%s%s%s", table, whereClause, orderBy, limitClause)
	args = append(args, limitArgs...)

	cxt, cancel := s.withTimeout(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(cxt, query, args...)
	if err != nil {
		return nil, span.Error(err)
	}
	defer rows.Close()

	var results []bson.Raw
	for rows.Next() {
		var docBytes []byte
		if err := rows.Scan(&docBytes); err != nil {
			return nil, span.Error(err)
		}

		doc, err := unmarshalDocument(docBytes)
		if err != nil {
			return nil, span.Error(err)
		}

		doc, err = applyProjection(doc, opts.Select)
		if err != nil {
			return nil, span.Error(err)
		}

		raw, err := bson.Marshal(doc)
		if err != nil {
			return nil, span.Error(err)
		}
		results = append(results, raw)
	}

	return results, span.Error(rows.Err())
}

func (s *Store) Insert(ctx context.Context, opts plugins.InsertOptions) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.EndSpan()
	if err := s.Connect(ctx); err != nil {
		return err
	}

	table, err := s.ensureCollection(ctx, opts.Collection)
	if err != nil {
		return span.Error(err)
	}

	query := fmt.Sprintf(`INSERT INTO %s (id, namespace, name, installation, runId, resultId, schemaType, schemaVersion, doc)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, table)

	for _, doc := range opts.Documents {
		id := ensureDocumentID(doc)
		docBytes, err := json.Marshal(doc)
		if err != nil {
			return span.Error(err)
		}

		values := docColumns(doc)
		args := []interface{}{
			id,
			nullStringValue(values.namespace),
			nullStringValue(values.name),
			nullStringValue(values.installation),
			nullStringValue(values.runID),
			nullStringValue(values.resultID),
			nullStringValue(values.schemaType),
			nullStringValue(values.schemaVersion),
			docBytes,
		}

		cxt, cancel := s.withTimeout(ctx)
		_, err = s.db.ExecContext(cxt, query, args...)
		cancel()
		if err != nil {
			return span.Error(err)
		}
	}

	return nil
}

func (s *Store) Patch(ctx context.Context, opts plugins.PatchOptions) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.EndSpan()
	if err := s.Connect(ctx); err != nil {
		return err
	}

	table, err := s.ensureCollection(ctx, opts.Collection)
	if err != nil {
		return span.Error(err)
	}

	id, doc, err := s.findOneDocument(ctx, table, opts.QueryDocument)
	if err != nil {
		return span.Error(err)
	}
	if id == "" {
		return nil
	}

	if err := applyPatch(doc, opts.Transformation); err != nil {
		return span.Error(err)
	}

	return span.Error(s.updateDocumentByID(ctx, table, id, doc))
}

func (s *Store) Remove(ctx context.Context, opts plugins.RemoveOptions) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.EndSpan()
	if err := s.Connect(ctx); err != nil {
		return err
	}

	table, err := s.ensureCollection(ctx, opts.Collection)
	if err != nil {
		return span.Error(err)
	}

	whereClause, args, err := s.buildWhereClause(opts.Filter)
	if err != nil {
		return span.Error(err)
	}
	if whereClause == "" {
		return span.Error(fmt.Errorf("refusing to remove documents without a filter"))
	}

	if opts.All {
		query := fmt.Sprintf("DELETE FROM %s%s", table, whereClause)
		cxt, cancel := s.withTimeout(ctx)
		_, err = s.db.ExecContext(cxt, query, args...)
		cancel()
		return span.Error(err)
	}

	id, _, err := s.findOneDocument(ctx, table, opts.Filter)
	if err != nil {
		return span.Error(err)
	}
	if id == "" {
		return nil
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE id = ?", table)
	cxt, cancel := s.withTimeout(ctx)
	_, err = s.db.ExecContext(cxt, query, id)
	cancel()
	return span.Error(err)
}

func (s *Store) Update(ctx context.Context, opts plugins.UpdateOptions) error {
	ctx, span := tracing.StartSpan(ctx)
	defer span.EndSpan()
	if err := s.Connect(ctx); err != nil {
		return err
	}

	table, err := s.ensureCollection(ctx, opts.Collection)
	if err != nil {
		return span.Error(err)
	}

	id, _, err := s.findOneDocument(ctx, table, opts.Filter)
	if err != nil {
		return span.Error(err)
	}

	if id == "" {
		if !opts.Upsert {
			return nil
		}
		return span.Error(s.Insert(ctx, plugins.InsertOptions{
			Collection: opts.Collection,
			Documents:  []bson.M{opts.Document},
		}))
	}

	if docID, ok := opts.Document["_id"].(string); ok && docID != "" && docID != id {
		return span.Error(fmt.Errorf("cannot change document _id during update"))
	}
	opts.Document["_id"] = id

	return span.Error(s.updateDocumentByID(ctx, table, id, opts.Document))
}

func (s *Store) ensureCollection(ctx context.Context, collection string) (string, error) {
	table, err := safeTableName(collection)
	if err != nil {
		return "", err
	}

	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	id TEXT PRIMARY KEY,
	namespace TEXT,
	name TEXT,
	installation TEXT,
	runId TEXT,
	resultId TEXT,
	schemaType TEXT,
	schemaVersion TEXT,
	doc BLOB NOT NULL
)`, table)

	cxt, cancel := s.withTimeout(ctx)
	_, err = s.db.ExecContext(cxt, query)
	cancel()
	if err != nil {
		return "", err
	}

	return table, nil
}

func (s *Store) indexDefinition(table string, keys bson.D) (string, []string, error) {
	columns := make([]string, 0, len(keys))
	nameParts := []string{table}

	for _, key := range keys {
		field := key.Key
		desc := false
		if strings.HasPrefix(field, "-") {
			desc = true
			field = strings.TrimPrefix(field, "-")
		}
		valueOrder := sortOrderFromValue(key.Value)
		if valueOrder < 0 {
			desc = true
		}

		column := columnExpression(field)
		if column == "" {
			continue
		}

		if desc {
			column = column + " DESC"
		}
		columns = append(columns, column)
		nameParts = append(nameParts, strings.ReplaceAll(field, ".", "_"))
	}

	if len(columns) == 0 {
		return "", nil, nil
	}

	indexName := "idx_" + strings.Join(nameParts, "_")
	return indexName, columns, nil
}

func (s *Store) buildWhereClause(filter bson.M) (string, []interface{}, error) {
	if len(filter) == 0 {
		return "", nil, nil
	}

	clauses := make([]string, 0, len(filter))
	args := make([]interface{}, 0, len(filter))
	for key, value := range filter {
		operator, ok := extractRegex(value)
		if ok {
			column := columnExpression(key)
			if column == "" {
				column = jsonExtract(key)
			}
			clauses = append(clauses, column+" LIKE ? ESCAPE '\\\\'")
			args = append(args, "%"+escapeLike(operator)+"%")
			continue
		}

		column := columnExpression(key)
		if column == "" {
			column = jsonExtract(key)
		}
		clauses = append(clauses, column+" = ?")
		args = append(args, value)
	}

	if len(clauses) == 0 {
		return "", nil, nil
	}

	return " WHERE " + strings.Join(clauses, " AND "), args, nil
}

func (s *Store) buildOrderBy(sort bson.D) (string, error) {
	if len(sort) == 0 {
		return "", nil
	}

	parts := make([]string, 0, len(sort))
	for _, entry := range sort {
		field := entry.Key
		desc := false
		if strings.HasPrefix(field, "-") {
			desc = true
			field = strings.TrimPrefix(field, "-")
		}
		if sortOrderFromValue(entry.Value) < 0 {
			desc = true
		}

		column := columnExpression(field)
		if column == "" {
			column = jsonExtract(field)
		}
		if desc {
			column = column + " DESC"
		}
		parts = append(parts, column)
	}

	return " ORDER BY " + strings.Join(parts, ", "), nil
}

func (s *Store) findOneDocument(ctx context.Context, table string, filter bson.M) (string, bson.M, error) {
	whereClause, args, err := s.buildWhereClause(filter)
	if err != nil {
		return "", nil, err
	}
	if whereClause == "" {
		return "", nil, fmt.Errorf("refusing to query without a filter")
	}

	query := fmt.Sprintf("SELECT id, doc FROM %s%s LIMIT 1", table, whereClause)
	cxt, cancel := s.withTimeout(ctx)
	defer cancel()
	row := s.db.QueryRowContext(cxt, query, args...)

	var id string
	var docBytes []byte
	if err := row.Scan(&id, &docBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, nil
		}
		return "", nil, err
	}

	doc, err := unmarshalDocument(docBytes)
	if err != nil {
		return "", nil, err
	}

	return id, doc, nil
}

func (s *Store) updateDocumentByID(ctx context.Context, table string, id string, doc bson.M) error {
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	values := docColumns(doc)
	query := fmt.Sprintf(`UPDATE %s
SET id = ?, namespace = ?, name = ?, installation = ?, runId = ?, resultId = ?, schemaType = ?, schemaVersion = ?, doc = ?
WHERE id = ?`, table)
	args := []interface{}{
		id,
		nullStringValue(values.namespace),
		nullStringValue(values.name),
		nullStringValue(values.installation),
		nullStringValue(values.runID),
		nullStringValue(values.resultID),
		nullStringValue(values.schemaType),
		nullStringValue(values.schemaVersion),
		docBytes,
		id,
	}

	cxt, cancel := s.withTimeout(ctx)
	_, err = s.db.ExecContext(cxt, query, args...)
	cancel()
	return err
}

func (s *Store) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.timeout)
}

type documentValues struct {
	namespace     sql.NullString
	name          sql.NullString
	installation  sql.NullString
	runID         sql.NullString
	resultID      sql.NullString
	schemaType    sql.NullString
	schemaVersion sql.NullString
}

func docColumns(doc bson.M) documentValues {
	return documentValues{
		namespace:     toNullString(doc["namespace"]),
		name:          toNullString(doc["name"]),
		installation:  toNullString(doc["installation"]),
		runID:         toNullString(doc["runId"]),
		resultID:      toNullString(doc["resultId"]),
		schemaType:    toNullString(doc["schemaType"]),
		schemaVersion: toNullString(doc["schemaVersion"]),
	}
}

func ensureDocumentID(doc bson.M) string {
	if id, ok := doc["_id"].(string); ok && id != "" {
		return id
	}

	id := cnab.NewULID()
	doc["_id"] = id
	return id
}

func unmarshalDocument(data []byte) (bson.M, error) {
	var doc bson.M
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	return doc, nil
}

func applyProjection(doc bson.M, selectDoc bson.D) (bson.M, error) {
	if len(selectDoc) == 0 {
		return doc, nil
	}

	include := map[string]bool{}
	exclude := map[string]bool{}
	for _, item := range selectDoc {
		val := projectionValue(item.Value)
		if val == nil {
			continue
		}
		if *val {
			include[item.Key] = true
		} else {
			exclude[item.Key] = true
		}
	}

	if len(include) > 0 {
		out := bson.M{}
		if _, ok := include["_id"]; !ok && !exclude["_id"] {
			include["_id"] = true
		}
		for key := range include {
			if value, ok := doc[key]; ok {
				out[key] = value
			}
		}
		return out, nil
	}

	if len(exclude) > 0 {
		out := bson.M{}
		for key, value := range doc {
			out[key] = value
		}
		for key := range exclude {
			delete(out, key)
		}
		return out, nil
	}

	return doc, nil
}

func applyPatch(doc bson.M, transformation bson.D) error {
	for _, op := range transformation {
		switch op.Key {
		case "$set":
			values, ok := asMap(op.Value)
			if !ok {
				return fmt.Errorf("invalid $set payload")
			}
			for key, value := range values {
				doc[key] = value
			}
		case "$unset":
			values, ok := asMap(op.Value)
			if !ok {
				return fmt.Errorf("invalid $unset payload")
			}
			for key := range values {
				delete(doc, key)
			}
		default:
			return errUnsupportedPatch
		}
	}
	return nil
}

func parseLastOutputsAggregate(pipeline []bson.D) (bson.M, bool) {
	if len(pipeline) != 3 {
		return nil, false
	}

	matchStage, ok := parseStage(pipeline[0], "$match")
	if !ok {
		return nil, false
	}
	if _, ok := parseStage(pipeline[1], "$sort"); !ok {
		return nil, false
	}
	groupStage, ok := parseStage(pipeline[2], "$group")
	if !ok {
		return nil, false
	}

	groupDoc, ok := asOrderedMap(groupStage)
	if !ok {
		return nil, false
	}
	if !isLastOutputGroup(groupDoc) {
		return nil, false
	}

	matchDoc, ok := asMap(matchStage)
	if !ok {
		return nil, false
	}
	return matchDoc, true
}

func isLastOutputGroup(groupDoc bson.D) bool {
	var hasID bool
	var hasLastOutput bool
	for _, entry := range groupDoc {
		switch entry.Key {
		case "_id":
			if entry.Value == "$name" {
				hasID = true
			}
		case "lastOutput":
			value, ok := asMap(entry.Value)
			if !ok {
				continue
			}
			if value["$first"] == "$$ROOT" {
				hasLastOutput = true
			}
		}
	}
	return hasID && hasLastOutput
}

func parseStage(stage bson.D, name string) (interface{}, bool) {
	if len(stage) != 1 {
		return nil, false
	}
	if stage[0].Key != name {
		return nil, false
	}
	return stage[0].Value, true
}

func asMap(value interface{}) (bson.M, bool) {
	switch v := value.(type) {
	case bson.M:
		return v, true
	case map[string]interface{}:
		return bson.M(v), true
	}
	return nil, false
}

func asOrderedMap(value interface{}) (bson.D, bool) {
	switch v := value.(type) {
	case bson.D:
		return v, true
	case []bson.E:
		return bson.D(v), true
	}
	return nil, false
}

func safeTableName(collection string) (string, error) {
	if !validTableName.MatchString(collection) {
		return "", fmt.Errorf("invalid collection name: %s", collection)
	}
	return collection, nil
}

func extractRegex(value interface{}) (string, bool) {
	raw, ok := value.(map[string]interface{})
	if !ok {
		if bsonVal, ok := value.(bson.M); ok {
			raw = bsonVal
		} else {
			return "", false
		}
	}

	if regex, ok := raw["$regex"].(string); ok {
		return regex, true
	}
	return "", false
}

func escapeLike(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "%", "\\%")
	value = strings.ReplaceAll(value, "_", "\\_")
	return value
}

func buildLimitClause(limit, skip int64) (string, []interface{}) {
	switch {
	case limit > 0 && skip > 0:
		return " LIMIT ? OFFSET ?", []interface{}{limit, skip}
	case limit > 0:
		return " LIMIT ?", []interface{}{limit}
	case skip > 0:
		return " LIMIT -1 OFFSET ?", []interface{}{skip}
	default:
		return "", nil
	}
}

func jsonExtract(field string) string {
	return fmt.Sprintf("json_extract(doc, '$.%s')", field)
}

func columnExpression(field string) string {
	switch field {
	case "_id":
		return "id"
	case "namespace", "name", "installation", "runId", "resultId", "schemaType", "schemaVersion":
		return field
	default:
		return ""
	}
}

func sortOrderFromValue(value interface{}) int {
	switch v := value.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		if v < 0 {
			return -1
		}
		return 1
	default:
		return 1
	}
}

func projectionValue(value interface{}) *bool {
	switch v := value.(type) {
	case bool:
		return &v
	case int:
		b := v != 0
		return &b
	case int32:
		b := v != 0
		return &b
	case int64:
		b := v != 0
		return &b
	case float64:
		b := v != 0
		return &b
	default:
		return nil
	}
}

func toNullString(value interface{}) sql.NullString {
	if value == nil {
		return sql.NullString{}
	}
	switch v := value.(type) {
	case string:
		if v == "" {
			return sql.NullString{}
		}
		return sql.NullString{String: v, Valid: true}
	case fmt.Stringer:
		str := v.String()
		if str == "" {
			return sql.NullString{}
		}
		return sql.NullString{String: str, Valid: true}
	default:
		str := fmt.Sprint(v)
		if str == "" {
			return sql.NullString{}
		}
		return sql.NullString{String: str, Valid: true}
	}
}

func nullStringValue(value sql.NullString) interface{} {
	if !value.Valid {
		return nil
	}
	return value.String
}
