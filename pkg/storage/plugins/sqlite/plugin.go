package sqlite

import (
	"fmt"
	"path/filepath"

	"get.porter.sh/porter/pkg/config"
	"get.porter.sh/porter/pkg/storage/plugins"
	"get.porter.sh/porter/pkg/storage/pluginstore"
	"github.com/hashicorp/go-plugin"
	"github.com/mitchellh/mapstructure"
)

const PluginKey = plugins.PluginInterface + ".porter.sqlite"

var _ plugins.StorageProtocol = &Store{}

// PluginConfig are the configuration settings that can be defined for the
// sqlite plugin in porter.yaml
type PluginConfig struct {
	// Path to the sqlite database file. Defaults to ~/.porter/porter.sqlite.
	Path string `mapstructure:"path"`
	// Timeout for database operations in seconds.
	Timeout int `mapstructure:"timeout,omitempty"`
}

func NewPlugin(c *config.Config, rawCfg interface{}) (plugin.Plugin, error) {
	cfg := PluginConfig{
		Timeout: 10,
	}
	if err := mapstructure.Decode(rawCfg, &cfg); err != nil {
		return nil, fmt.Errorf("error reading plugin configuration: %w", err)
	}

	if cfg.Path == "" {
		home, err := c.GetHomeDir()
		if err != nil {
			return nil, fmt.Errorf("could not determine porter home directory: %w", err)
		}
		cfg.Path = filepath.Join(home, "porter.sqlite")
	}

	impl := NewStore(c.Context, cfg)
	return pluginstore.NewPlugin(c.Context, impl), nil
}
