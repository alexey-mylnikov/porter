package porter

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	"get.porter.sh/porter/pkg/cnab"
	"get.porter.sh/porter/pkg/tracing"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/daemon"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

const ociRefNameAnnotation = "org.opencontainers.image.ref.name"

func (p *Porter) loadBundleFromArchive(ctx context.Context, archiveFile string) (cnab.BundleReference, error) {
	ctx, log := tracing.StartSpan(ctx)
	defer log.EndSpan()

	source := p.FileSystem.Abs(archiveFile)
	tmpDir, err := p.FileSystem.TempDir("", "porter")
	if err != nil {
		return cnab.BundleReference{}, log.Errorf("error creating temp directory for archive extraction: %w", err)
	}
	defer func() {
		err = errors.Join(err, p.FileSystem.RemoveAll(tmpDir))
	}()

	bundleRef, err := p.extractBundle(ctx, tmpDir, source)
	if err != nil {
		return cnab.BundleReference{}, err
	}

	extractedDir := filepath.Join(tmpDir, strings.TrimSuffix(filepath.Base(source), ".tgz"))
	layoutDir := filepath.Join(extractedDir, "artifacts/layout")

	loaded, imageRefs, err := loadImagesFromLayout(ctx, layoutDir)
	if err != nil {
		return cnab.BundleReference{}, log.Errorf("failed to load images from archive %s: %w", archiveFile, err)
	}

	if bundleRef.RelocationMap == nil {
		bundleRef.RelocationMap = make(map[string]string)
	}
	for originalRef, tagRef := range imageRefs {
		bundleRef.RelocationMap[originalRef] = tagRef
	}
	for i := range bundleRef.Definition.InvocationImages {
		// Offline installs load images into the local daemon without repo digests,
		// so skip digest validation against repoDigests.
		bundleRef.Definition.InvocationImages[i].Digest = ""
	}

	log.Infof("Loaded %d images from archive %s into the local Docker cache.", loaded, archiveFile)
	return bundleRef, nil
}

func loadImagesFromLayout(ctx context.Context, layoutDir string) (int, map[string]string, error) {
	layoutPath, err := layout.FromPath(layoutDir)
	if err != nil {
		return 0, nil, err
	}
	imageIndex, err := layoutPath.ImageIndex()
	if err != nil {
		return 0, nil, err
	}
	indexManifest, err := imageIndex.IndexManifest()
	if err != nil {
		return 0, nil, err
	}

	refDescriptors := map[string][]v1.Descriptor{}
	for _, desc := range indexManifest.Manifests {
		if desc.Annotations == nil {
			return 0, nil, fmt.Errorf("missing %s annotation for image descriptor %s", ociRefNameAnnotation, desc.Digest.String())
		}
		refName := desc.Annotations[ociRefNameAnnotation]
		if refName == "" {
			return 0, nil, fmt.Errorf("missing %s annotation for image descriptor %s", ociRefNameAnnotation, desc.Digest.String())
		}
		refDescriptors[refName] = append(refDescriptors[refName], desc)
	}

	platform := v1.Platform{OS: runtime.GOOS, Architecture: runtime.GOARCH}
	loaded := 0
	imageRefs := make(map[string]string, len(refDescriptors))
	for refName, descs := range refDescriptors {
		desc := selectDescriptorForPlatform(descs, platform)

		img, err := imageFromDescriptor(imageIndex, desc, platform)
		if err != nil {
			return 0, nil, fmt.Errorf("failed to read image %s from OCI layout: %w", refName, err)
		}

		tag, tagRef, err := resolveLoadTag(refName)
		if err != nil {
			return 0, nil, fmt.Errorf("unable to resolve tag for %s: %w", refName, err)
		}

		if _, err := daemon.Write(tag, img, daemon.WithContext(ctx)); err != nil {
			return 0, nil, fmt.Errorf("unable to load image %s into docker as %s: %w", refName, tagRef, err)
		}
		imageRefs[refName] = tagRef
		loaded++
	}

	return loaded, imageRefs, nil
}

func resolveLoadTag(refName string) (name.Tag, string, error) {
	ref, err := cnab.ParseOCIReference(refName)
	if err != nil {
		return name.Tag{}, "", err
	}

	var tagRef cnab.OCIReference
	if ref.HasTag() {
		repo, err := cnab.ParseOCIReference(ref.Repository())
		if err != nil {
			return name.Tag{}, "", err
		}
		tagRef, err = repo.WithTag(ref.Tag())
		if err != nil {
			return name.Tag{}, "", err
		}
	} else {
		tagRef, err = cnab.CalculateTemporaryImageTag(ref)
		if err != nil {
			return name.Tag{}, "", err
		}
	}

	tag, err := name.NewTag(tagRef.String(), name.WeakValidation)
	if err != nil {
		return name.Tag{}, "", err
	}
	return tag, tagRef.String(), nil
}

func selectDescriptorForPlatform(descs []v1.Descriptor, platform v1.Platform) v1.Descriptor {
	for _, desc := range descs {
		if desc.Platform != nil && desc.Platform.Equals(platform) {
			return desc
		}
	}
	for _, desc := range descs {
		if desc.Platform == nil {
			return desc
		}
	}
	return descs[0]
}

func imageFromDescriptor(index v1.ImageIndex, desc v1.Descriptor, platform v1.Platform) (v1.Image, error) {
	switch desc.MediaType {
	case types.OCIImageIndex, types.DockerManifestList:
		idx, err := index.ImageIndex(desc.Digest)
		if err != nil {
			return nil, err
		}
		return imageFromIndex(idx, platform)
	default:
		return index.Image(desc.Digest)
	}
}

func imageFromIndex(index v1.ImageIndex, platform v1.Platform) (v1.Image, error) {
	manifest, err := index.IndexManifest()
	if err != nil {
		return nil, err
	}
	for _, desc := range manifest.Manifests {
		if desc.Platform != nil && desc.Platform.Equals(platform) {
			return imageFromDescriptor(index, desc, platform)
		}
	}
	if len(manifest.Manifests) == 0 {
		return nil, errors.New("image index has no manifests")
	}
	return imageFromDescriptor(index, manifest.Manifests[0], platform)
}
