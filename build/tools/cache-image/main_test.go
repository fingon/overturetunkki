package main

import (
	"archive/tar"
	"bytes"
	"io"
	"path/filepath"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"gotest.tools/v3/assert"
)

const imageTag = "example.com/service:test"

func TestCacheImage(t *testing.T) {
	directory := t.TempDir()
	tag, err := name.NewTag(imageTag)
	assert.NilError(t, err)
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	assert.NilError(t, writer.WriteHeader(&tar.Header{Name: "existing/", Typeflag: tar.TypeDir, Mode: 0o755}))
	assert.NilError(t, writer.Close())
	baseLayer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(buffer.Bytes())), nil
	})
	assert.NilError(t, err)
	base, err := mutate.AppendLayers(empty.Image, baseLayer)
	assert.NilError(t, err)
	baseConfig, err := base.ConfigFile()
	assert.NilError(t, err)
	baseConfig.Architecture = "arm64"
	baseConfig.OS = "linux"
	baseConfig.Config.User = "65532"
	baseConfig.Config.Entrypoint = []string{"/ko-app/service"}
	baseConfig.Config.Env = []string{"KO_DATA_PATH=/var/run/ko"}
	base, err = mutate.ConfigFile(base, baseConfig)
	assert.NilError(t, err)
	input := filepath.Join(directory, "input.tar")
	assert.NilError(t, tarball.WriteToFile(input, tag, base))
	config := imageConfig{Input: input, Output: filepath.Join(directory, "output.tar"), Tag: imageTag}
	assert.NilError(t, config.build())
	image, err := tarball.ImageFromPath(config.Output, &tag)
	assert.NilError(t, err)
	imageConfig, err := image.ConfigFile()
	assert.NilError(t, err)
	assert.DeepEqual(t, imageConfig.Config, baseConfig.Config)
	assert.Equal(t, imageConfig.Architecture, baseConfig.Architecture)
	assert.Equal(t, imageConfig.OS, baseConfig.OS)
	layers, err := image.Layers()
	assert.NilError(t, err)
	assert.Equal(t, len(layers), 2)
	baseDigest, err := baseLayer.Digest()
	assert.NilError(t, err)
	preservedDigest, err := layers[0].Digest()
	assert.NilError(t, err)
	assert.Equal(t, preservedDigest, baseDigest)
	reader, err := layers[1].Uncompressed()
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, reader.Close()) })
	archive := tar.NewReader(reader)
	header, err := archive.Next()
	assert.NilError(t, err)
	assert.Equal(t, header.Name, "var/cache/overture/")
	assert.Equal(t, header.Typeflag, byte(tar.TypeDir))
	assert.Equal(t, header.Mode, int64(0o750))
	assert.Equal(t, header.Uid, cacheOwnerID)
	assert.Equal(t, header.Gid, cacheOwnerID)
	assert.Equal(t, header.Size, int64(0))
	_, err = archive.Next()
	assert.Equal(t, err, io.EOF)
	firstDigest, err := image.Digest()
	assert.NilError(t, err)
	assert.NilError(t, config.build())
	image, err = tarball.ImageFromPath(config.Output, &tag)
	assert.NilError(t, err)
	secondDigest, err := image.Digest()
	assert.NilError(t, err)
	assert.Equal(t, secondDigest, firstDigest)
}

func TestCacheImageErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		tag  string
		want string
	}{
		{name: "malformed reference", tag: "invalid tag", want: "parse image tag"},
		{name: "missing archive", tag: imageTag, want: "read ko image"},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := imageConfig{Input: filepath.Join(t.TempDir(), "missing.tar"), Tag: test.tag}
			assert.ErrorContains(t, config.build(), test.want)
		})
	}
}
