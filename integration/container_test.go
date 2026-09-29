package integration

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mstenber/overturetunkki/internal/geoparquet"
	"github.com/uber/h3-go/v4"
	"gotest.tools/v3/assert"
)

const (
	fixtureRelease      = "2026-09-23.1"
	rolloverRelease     = "2026-09-24.0"
	containerImageEnv   = "OVERTURE_SERVICE_IMAGE"
	containerTestEnv    = "OVERTURE_CONTAINER_TEST"
	clientBinaryEnv     = "OVERTURE_CLIENT_BIN"
	containerName       = "overturetunkki-container-test"
	containerListenPort = 8080
)

type fixtureMode string

const (
	fixtureNormal      fixtureMode = "normal"
	fixtureCatalogDown fixtureMode = "catalog-down"
	fixtureAssetDown   fixtureMode = "asset-down"
)

type fixtureServer struct {
	t        *testing.T
	server   *http.Server
	listener net.Listener
	baseURL  string
	certPath string

	mu         sync.Mutex
	mode       fixtureMode
	release    string
	assetDelay time.Duration
	assetBytes []byte
}

func TestContainerLifecycle(t *testing.T) {
	if os.Getenv(containerTestEnv) != "1" {
		t.Skip("set OVERTURE_CONTAINER_TEST=1 to run the Docker integration test")
	}
	image := os.Getenv(containerImageEnv)
	if image == "" {
		image = "overturetunkki/service:dev"
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("Docker is required for the opt-in container test: %v", err)
	}
	if err := runCommand("docker", "image", "inspect", image); err != nil {
		t.Fatalf("inspect service image %q: %v", image, err)
	}
	clientBinary := os.Getenv(clientBinaryEnv)
	if clientBinary == "" {
		clientBinary = filepath.Join("..", "bin", "overture-client")
	}
	if _, err := os.Stat(clientBinary); err != nil {
		t.Fatalf("inspect CLI binary %q: %v; run make build first", clientBinary, err)
	}

	fixture := newFixtureServer(t)
	cacheDirectory := t.TempDir()
	if err := os.Chmod(cacheDirectory, 0o777); err != nil {
		t.Fatalf("make cache directory writable: %v", err)
	}
	container := newServiceContainer(t, image, fixture, cacheDirectory)
	container.start()
	defer container.remove()

	client := &http.Client{Timeout: 3 * time.Second}
	waitForReady(t, client, container.url("/readyz"))
	oldCatalog := fetchCatalog(t, client, container.url("/v1/catalog"))
	cell := testCell(t, 9)
	tileBody, tileETag := fetchTile(t, client, container, oldCatalog.CatalogVersion, cell, http.StatusOK)
	validateTile(t, tileBody)

	t.Run("CLI download and conditional response", func(t *testing.T) {
		outputPath := filepath.Join(t.TempDir(), "tile.parquet")
		stdout, stderr, err := runCLI(t, clientBinary, "--server-url="+container.url(""), "--timeout=30s", "tile", cell.String(), "--output="+outputPath)
		assert.NilError(t, err, string(stderr))
		if err != nil {
			return
		}
		var result cliTileResult
		assert.NilError(t, json.Unmarshal(stdout, &result))
		assert.Equal(t, result.Status, http.StatusOK)
		assert.Equal(t, result.CatalogVersion, oldCatalog.CatalogVersion)
		downloadedBody, readErr := os.ReadFile(outputPath)
		assert.NilError(t, readErr)
		assert.DeepEqual(t, downloadedBody, tileBody)
		validateTile(t, downloadedBody)

		stdout, stderr, err = runCLI(t, clientBinary, "--server-url="+container.url(""), "--timeout=30s", "tile", cell.String(), "--if-none-match="+tileETag, "--output="+outputPath)
		assert.NilError(t, err, string(stderr))
		if err != nil {
			return
		}
		assert.NilError(t, json.Unmarshal(stdout, &result))
		assert.Equal(t, result.Status, http.StatusNotModified)
		assert.Assert(t, result.NotModified)
		unchangedBody, readErr := os.ReadFile(outputPath)
		assert.NilError(t, readErr)
		assert.DeepEqual(t, unchangedBody, downloadedBody)
	})

	t.Run("concurrent clients", func(t *testing.T) {
		const clientCount = 8
		errors := make(chan error, clientCount)
		var waitGroup sync.WaitGroup
		for range clientCount {
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				response, err := getTile(client, container, oldCatalog.CatalogVersion, cell, "")
				if err != nil {
					errors <- err
					return
				}
				if response.StatusCode != http.StatusOK {
					errors <- fmt.Errorf("concurrent tile returned %s", response.Status)
					return
				}
				if _, err := io.Copy(io.Discard, response.Body); err != nil {
					errors <- fmt.Errorf("read concurrent tile: %w", err)
					return
				}
				if err := response.Body.Close(); err != nil {
					errors <- fmt.Errorf("close concurrent tile: %w", err)
				}
			}()
		}
		waitGroup.Wait()
		close(errors)
		for err := range errors {
			assert.NilError(t, err)
		}
	})

	t.Run("conditional response", func(t *testing.T) {
		response, err := getTile(client, container, oldCatalog.CatalogVersion, cell, tileETag)
		assert.NilError(t, err)
		if err != nil {
			return
		}
		assert.Equal(t, response.StatusCode, http.StatusNotModified)
		assert.NilError(t, response.Body.Close())
	})

	fixture.setRelease(rolloverRelease)
	waitForStatus(t, client, func() int {
		response, err := getTile(client, container, oldCatalog.CatalogVersion, cell, "")
		if err != nil {
			return 0
		}
		defer response.Body.Close()
		return response.StatusCode
	}, http.StatusConflict)
	newCatalog := fetchCatalog(t, client, container.url("/v1/catalog"))
	assert.Equal(t, newCatalog.Release, rolloverRelease)
	t.Run("CLI reports stale catalog version", func(t *testing.T) {
		outputPath := filepath.Join(t.TempDir(), "stale.parquet")
		_, stderr, err := runCLI(t, clientBinary, "--server-url="+container.url(""), "--timeout=30s", "tile", cell.String(), "--catalog-version="+oldCatalog.CatalogVersion, "--output="+outputPath)
		assert.Assert(t, err != nil, string(stderr))
		var exitErr *exec.ExitError
		assert.Assert(t, errors.As(err, &exitErr), string(stderr))
		if exitErr != nil {
			assert.Equal(t, exitErr.ExitCode(), 3)
		}
		assert.Assert(t, strings.Contains(string(stderr), "current catalog version"), string(stderr))
		_, statErr := os.Stat(outputPath)
		assert.Assert(t, os.IsNotExist(statErr))
	})
	newBody, _ := fetchTile(t, client, container, newCatalog.CatalogVersion, cell, http.StatusOK)
	validateTile(t, newBody)

	fixture.setMode(fixtureCatalogDown)
	waitForStatus(t, client, func() int {
		response, err := client.Get(container.url("/v1/catalog"))
		if err != nil {
			return 0
		}
		defer response.Body.Close()
		return response.StatusCode
	}, http.StatusServiceUnavailable)
	waitForStatus(t, client, func() int {
		response, err := client.Get(container.url("/readyz"))
		if err != nil {
			return 0
		}
		defer response.Body.Close()
		return response.StatusCode
	}, http.StatusServiceUnavailable)
	fixture.setMode(fixtureNormal)
	waitForReady(t, client, container.url("/readyz"))
	fixture.setMode(fixtureAssetDown)
	assetFailureCell := testCell(t, 7)
	fetchTile(t, client, container, newCatalog.CatalogVersion, assetFailureCell, http.StatusServiceUnavailable)
	fixture.setMode(fixtureNormal)

	if runtime.GOOS == "linux" {
		t.Run("worker death and replacement", func(t *testing.T) {
			fixture.setAssetDelay(5 * time.Second)
			uncachedCell := testCell(t, 8)
			responseDone := make(chan *http.Response, 1)
			go func() {
				response, err := getTile(client, container, newCatalog.CatalogVersion, uncachedCell, "")
				if err != nil {
					responseDone <- nil
					return
				}
				responseDone <- response
			}()
			workerPID := waitForWorkerPID(t, container.name)
			assert.NilError(t, killProcess(workerPID))
			select {
			case response := <-responseDone:
				if response != nil {
					assert.Assert(t, response.StatusCode >= http.StatusInternalServerError)
					assert.NilError(t, response.Body.Close())
				}
			case <-time.After(10 * time.Second):
				t.Fatal("worker death did not finish the tile request")
			}
			fixture.setAssetDelay(0)
			body, _ := fetchTile(t, client, container, newCatalog.CatalogVersion, uncachedCell, http.StatusOK)
			validateTile(t, body)
		})
	} else {
		t.Log("worker PID replacement is only exercised on Linux Docker hosts")
	}

	container.remove()
	container = newServiceContainer(t, image, fixture, cacheDirectory)
	container.start()
	defer container.remove()
	waitForReady(t, client, container.url("/readyz"))
	restartedBody, _ := fetchTile(t, client, container, newCatalog.CatalogVersion, cell, http.StatusOK)
	validateTile(t, restartedBody)
}

type serviceContainer struct {
	t              *testing.T
	image          string
	fixture        *fixtureServer
	cacheDirectory string
	certPath       string
	name           string
	hostPort       int
}

type cliTileResult struct {
	Status         int    `json:"status"`
	CatalogVersion string `json:"catalog_version"`
	NotModified    bool   `json:"not_modified"`
}

func newServiceContainer(t *testing.T, image string, fixture *fixtureServer, cacheDirectory string) *serviceContainer {
	t.Helper()
	return &serviceContainer{
		t:              t,
		image:          image,
		fixture:        fixture,
		cacheDirectory: cacheDirectory,
		certPath:       fixture.certPath,
		name:           containerName,
		hostPort:       reservePort(t),
	}
}

func (container *serviceContainer) start() {
	container.t.Helper()
	args := []string{
		"run", "--detach", "--name", container.name,
		"--read-only",
		"--user", "65532:65532",
		"--security-opt", "no-new-privileges",
		"--cap-drop", "ALL",
		"--add-host", "host.docker.internal:host-gateway",
		"--publish", fmt.Sprintf("127.0.0.1:%d:%d", container.hostPort, containerListenPort),
		"--mount", "type=bind,src=" + container.cacheDirectory + ",dst=/var/cache/overture",
		"--mount", "type=bind,src=" + container.certPath + ",dst=/tmp/container-test-ca.pem,readonly",
		"--tmpfs", "/tmp:rw,noexec,nosuid,size=16m",
		"--env", "KO_DATA_PATH=/ko-app",
		"--env", "SSL_CERT_FILE=/tmp/container-test-ca.pem",
		"--env", "OVERTURE_LISTEN=:8080",
		"--env", "OVERTURE_CACHE_DIR=/var/cache/overture",
		"--env", "OVERTURE_CATALOG_URL=" + container.fixture.baseURL + "/catalog.json",
		"--env", "OVERTURE_CATALOG_HOST=" + hostPort(container.fixture.baseURL),
		"--env", "OVERTURE_ASSET_HOST=" + hostPort(container.fixture.baseURL),
		"--env", "OVERTURE_CATALOG_POLL_INTERVAL=100ms",
		"--env", "OVERTURE_CATALOG_TIMEOUT=2s",
		"--env", "OVERTURE_FIELDS=id,geometry,names",
		"--env", "OVERTURE_MAX_TILE_BYTES=1048576",
		"--env", "OVERTURE_MAX_TILE_ROWS=1000",
		"--env", "OVERTURE_CACHE_MAX_BYTES=16777216",
		"--env", "OVERTURE_CACHE_MAX_ENTRIES=32",
		"--env", "OVERTURE_SCRATCH_MAX_BYTES=16777216",
		"--env", "OVERTURE_WORKER_COUNT=2",
		"--env", "OVERTURE_WORKER_MEMORY_BYTES=134217728",
		"--env", "OVERTURE_WORKER_THREADS=1",
		"--env", "OVERTURE_QUEUE_CAPACITY=8",
		"--env", "OVERTURE_TILE_TIMEOUT=10s",
		"--env", "OVERTURE_NEGATIVE_CACHE_ENTRIES=8",
		"--env", "OVERTURE_NEGATIVE_CACHE_TTL=1m",
		"--env", "OVERTURE_WRITE_TIMEOUT=5s",
		container.image,
	}
	if output, err := commandOutput("docker", args...); err != nil {
		container.t.Fatalf("start service container: %v\n%s", err, output)
	}
}

func (container *serviceContainer) remove() {
	container.t.Helper()
	if err := runCommand("docker", "rm", "--force", container.name); err != nil {
		container.t.Logf("remove service container: %v", err)
	}
}

func (container *serviceContainer) url(path string) string {
	return "http://127.0.0.1:" + strconv.Itoa(container.hostPort) + path
}

func newFixtureServer(t *testing.T) *fixtureServer {
	t.Helper()
	certificate, key, err := fixtureCertificate()
	if err != nil {
		t.Fatalf("create fixture certificate: %v", err)
	}
	certificateFile := filepath.Join(t.TempDir(), "fixture-ca.pem")
	if err := os.WriteFile(certificateFile, certificate, 0o644); err != nil {
		t.Fatalf("write fixture certificate: %v", err)
	}
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen for fixture server: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	fixture := &fixtureServer{
		t:          t,
		listener:   listener,
		baseURL:    "https://host.docker.internal:" + strconv.Itoa(port),
		certPath:   certificateFile,
		release:    fixtureRelease,
		assetBytes: fixtureFile(t, filepath.Join("..", "testdata", "geoparquet", "places.parquet")),
	}
	keyPair, err := tlsKeyPair(certificate, key)
	if err != nil {
		listener.Close()
		t.Fatalf("create fixture TLS key pair: %v", err)
	}
	fixture.server = &http.Server{Handler: http.HandlerFunc(fixture.serveHTTP)}
	tlsListener := tls.NewListener(listener, &tls.Config{Certificates: []tls.Certificate{keyPair}, MinVersion: tls.VersionTLS13})
	go func() {
		if err := fixture.server.Serve(tlsListener); err != nil && err != http.ErrServerClosed {
			t.Errorf("fixture server failed: %v", err)
		}
	}()
	t.Cleanup(func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := fixture.server.Shutdown(shutdownContext); err != nil {
			t.Errorf("shutdown fixture server: %v", err)
		}
	})
	return fixture
}

func (fixture *fixtureServer) serveHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	fixture.mu.Lock()
	mode := fixture.mode
	release := fixture.release
	delay := fixture.assetDelay
	fixture.mu.Unlock()
	isAsset := strings.HasSuffix(request.URL.Path, ".parquet")
	if mode == fixtureCatalogDown && !isAsset {
		http.Error(responseWriter, "catalog outage", http.StatusServiceUnavailable)
		return
	}
	if mode == fixtureAssetDown && isAsset {
		http.Error(responseWriter, "asset outage", http.StatusServiceUnavailable)
		return
	}
	if isAsset {
		if delay > 0 {
			timer := time.NewTimer(delay)
			<-timer.C
		}
		responseWriter.Header().Set("Content-Type", "application/vnd.apache.parquet")
		responseWriter.Header().Set("ETag", "\"fixture-asset\"")
		responseWriter.Header().Set("Content-Length", strconv.Itoa(len(fixture.assetBytes)))
		responseWriter.WriteHeader(http.StatusOK)
		if request.Method != http.MethodHead {
			if _, err := responseWriter.Write(fixture.assetBytes); err != nil {
				fixture.t.Logf("write fixture asset: %v", err)
			}
		}
		return
	}
	body, err := fixture.document(request.URL.Path, release)
	if err != nil {
		http.NotFound(responseWriter, request)
		return
	}
	etag := fmt.Sprintf("\"fixture-%x\"", sha256Bytes(body))
	if request.Header.Get("If-None-Match") == etag {
		responseWriter.Header().Set("ETag", etag)
		responseWriter.WriteHeader(http.StatusNotModified)
		return
	}
	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.Header().Set("ETag", etag)
	responseWriter.WriteHeader(http.StatusOK)
	if request.Method != http.MethodHead {
		if _, err := responseWriter.Write(body); err != nil {
			fixture.t.Logf("write fixture document: path=%s error=%v", request.URL.Path, err)
		}
	}
}

func (fixture *fixtureServer) document(requestPath, release string) ([]byte, error) {
	fixturePath := requestPath
	if requestPath == "/catalog.json" {
		fixturePath = "/catalog.json"
	} else {
		fixturePath = strings.Replace(requestPath, "/"+release+"/", "/"+fixtureRelease+"/", 1)
	}
	fileName := filepath.Join("..", "testdata", "stac", strings.TrimPrefix(fixturePath, "/"))
	body, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	body = bytes.ReplaceAll(body, []byte("https://stac.overturemaps.org"), []byte(fixture.baseURL))
	body = bytes.ReplaceAll(body, []byte("https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com"), []byte(fixture.baseURL))
	body = bytes.ReplaceAll(body, []byte(fixtureRelease), []byte(release))
	return body, nil
}

func (fixture *fixtureServer) setRelease(release string) {
	fixture.mu.Lock()
	fixture.release = release
	fixture.mu.Unlock()
}

func (fixture *fixtureServer) setMode(mode fixtureMode) {
	fixture.mu.Lock()
	fixture.mode = mode
	fixture.mu.Unlock()
}

func (fixture *fixtureServer) setAssetDelay(delay time.Duration) {
	fixture.mu.Lock()
	fixture.assetDelay = delay
	fixture.mu.Unlock()
}

type wireCatalog struct {
	Release        string `json:"release"`
	CatalogVersion string `json:"catalog_version"`
	ProjectionID   string `json:"projection_id"`
}

func fetchCatalog(t *testing.T, client *http.Client, endpoint string) wireCatalog {
	t.Helper()
	response, err := client.Get(endpoint)
	assert.NilError(t, err)
	if err != nil {
		return wireCatalog{}
	}
	defer response.Body.Close()
	assert.Equal(t, response.StatusCode, http.StatusOK)
	var result wireCatalog
	assert.NilError(t, json.NewDecoder(response.Body).Decode(&result))
	return result
}

func fetchTile(t *testing.T, client *http.Client, container *serviceContainer, version string, cell h3.Cell, wantStatus int) ([]byte, string) {
	t.Helper()
	response, err := getTile(client, container, version, cell, "")
	assert.NilError(t, err)
	if err != nil {
		return nil, ""
	}
	defer response.Body.Close()
	assert.Equal(t, response.StatusCode, wantStatus)
	body, err := io.ReadAll(response.Body)
	assert.NilError(t, err)
	return body, response.Header.Get("ETag")
}

func getTile(client *http.Client, container *serviceContainer, version string, cell h3.Cell, etag string) (*http.Response, error) {
	query := url.Values{"catalog_version": []string{version}}
	request, err := http.NewRequest(http.MethodGet, container.url("/v1/tiles/places/"+cell.String()+"?"+query.Encode()), nil)
	if err != nil {
		return nil, err
	}
	if etag != "" {
		request.Header.Set("If-None-Match", etag)
	}
	return client.Do(request)
}

func validateTile(t *testing.T, body []byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tile.parquet")
	assert.NilError(t, os.WriteFile(path, body, 0o600))
	validation, err := geoparquet.ValidateFile(path, []string{"id", "geometry", "names"}, 1<<20)
	assert.NilError(t, err)
	if err == nil {
		assert.Assert(t, validation.SizeBytes > 0)
	}
}

func runCLI(t *testing.T, binary string, args ...string) ([]byte, []byte, error) {
	t.Helper()
	context, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(context, binary, args...)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

func waitForReady(t *testing.T, client *http.Client, endpoint string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(endpoint)
		if err == nil {
			status := response.StatusCode
			response.Body.Close()
			if status == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("service did not become ready; logs:\n%s", dockerLogs(t, containerName))
}

func waitForStatus(t *testing.T, client *http.Client, status func() int, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if status() == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("service did not return status %d", want)
}

func waitForWorkerPID(t *testing.T, name string) int {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		output, err := commandOutput("docker", "top", name, "-eo", "pid,args")
		if err == nil {
			for _, line := range strings.Split(output, "\n") {
				if !strings.Contains(line, "--mode=worker") {
					continue
				}
				fields := strings.Fields(line)
				if len(fields) == 0 {
					continue
				}
				pid, err := strconv.Atoi(fields[0])
				if err == nil && pid > 0 {
					return pid
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("service did not start an isolated worker; logs:\n%s", dockerLogs(t, name))
	return 0
}

func testCell(t testing.TB, resolution int) h3.Cell {
	t.Helper()
	cell, err := h3.LatLngToCell(h3.NewLatLng(37.775938728915946, -122.41795063018799), resolution)
	assert.NilError(t, err)
	return cell
}

func reservePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NilError(t, err)
	if err != nil {
		return 0
	}
	port := listener.Addr().(*net.TCPAddr).Port
	assert.NilError(t, listener.Close())
	return port
}

func hostPort(baseURL string) string {
	return strings.TrimPrefix(baseURL, "https://")
}

func fixtureCertificate() ([]byte, []byte, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return nil, nil, fmt.Errorf("generate certificate serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "host.docker.internal"},
		DNSNames:     []string{"host.docker.internal"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, nil, fmt.Errorf("create certificate: %w", err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}), nil
}

func tlsKeyPair(certificate, key []byte) (tls.Certificate, error) {
	return tls.X509KeyPair(certificate, key)
}

func sha256Bytes(value []byte) []byte {
	digest := sha256.Sum256(value)
	return digest[:]
}

func killProcess(pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("find worker process %d: %w", pid, err)
	}
	if err := process.Kill(); err != nil {
		return fmt.Errorf("kill worker process %d: %w", pid, err)
	}
	return nil
}

func dockerLogs(t *testing.T, name string) string {
	t.Helper()
	output, err := commandOutput("docker", "logs", name)
	if err != nil {
		return fmt.Sprintf("unable to read logs: %v", err)
	}
	return output
}

func runCommand(name string, args ...string) error {
	_, err := commandOutput(name, args...)
	return err
}

func commandOutput(name string, args ...string) (string, error) {
	command := exec.Command(name, args...)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	err := command.Run()
	return output.String(), err
}

func fixtureFile(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("..", "testdata", name)
	body, err := os.ReadFile(path)
	assert.NilError(t, err)
	return body
}
