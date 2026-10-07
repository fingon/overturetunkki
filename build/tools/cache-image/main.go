package main

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/alecthomas/kong"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

const cacheOwnerID = 65532

type imageConfig struct {
	Input   string `help:"Input ko image archive." required:""`
	Output  string `help:"Output service image archive." required:""`
	Tag     string `help:"Service image tag." required:""`
	Verbose bool   `help:"Enable debug logging." short:"v"`
}

func main() {
	var config imageConfig
	kong.Parse(&config)
	if config.Verbose {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	if err := config.build(); err != nil {
		slog.Error("build service cache image", "error", err)
		os.Exit(1)
	}
}

func (config imageConfig) build() (err error) {
	tag, err := name.NewTag(config.Tag)
	if err != nil {
		return fmt.Errorf("parse image tag: %w", err)
	}
	image, err := tarball.ImageFromPath(config.Input, &tag)
	if err != nil {
		return fmt.Errorf("read ko image: %w", err)
	}
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	if err := writer.WriteHeader(&tar.Header{
		Name: "var/cache/overture/", Typeflag: tar.TypeDir,
		Mode: 0o750, Uid: cacheOwnerID, Gid: cacheOwnerID,
	}); err != nil {
		return fmt.Errorf("write cache directory header: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close cache directory layer: %w", err)
	}
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(buffer.Bytes())), nil
	})
	if err != nil {
		return fmt.Errorf("create cache directory layer: %w", err)
	}
	image, err = mutate.AppendLayers(image, layer)
	if err != nil {
		return fmt.Errorf("append cache directory layer: %w", err)
	}
	output, err := os.CreateTemp(filepath.Dir(config.Output), ".cache-image-*.tar")
	if err != nil {
		return fmt.Errorf("create output archive: %w", err)
	}
	defer func() {
		if removeErr := os.Remove(output.Name()); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("remove temporary image archive: %w", removeErr))
		}
	}()
	writeErr := tarball.Write(tag, image, output)
	closeErr := output.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		return fmt.Errorf("write image archive: %w", err)
	}
	if err := os.Rename(output.Name(), config.Output); err != nil {
		return fmt.Errorf("publish image archive: %w", err)
	}
	slog.Debug("added writable cache directory", "archive", config.Output, "tag", config.Tag)
	return nil
}
