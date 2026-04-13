package consumption

import (
	"archive/zip"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	flexTestImage          = "goworker-flex-test:latest"
	flexTestIdealImage     = "goworker-flex-test-ideal:latest"
	flexTestIdealWAImage   = "goworker-flex-test-ideal-wa:latest"
	flexTestHostFixImage   = "goworker-flex-test-hostfix:latest"
	dummyContKey           = "MDEyMzQ1Njc4OUFCQ0RFRjAxMjM0NTY3ODlBQkNERUY="
	encryptionIV           = "0123456789abcedf"
)

// repoRoot returns the absolute path to the repository root.
func repoRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..")
}

// samplesDir returns the absolute path to the samples directory.
func samplesDir() string {
	return filepath.Join(repoRoot(), "samples")
}

// flexContainer manages a flex consumption Docker container for testing.
type flexContainer struct {
	id   string
	port string
	t    testing.TB
}

func requireDocker(t testing.TB) {
	t.Helper()
	if out, err := exec.Command("docker", "info").CombinedOutput(); err != nil {
		t.Fatalf("docker is not available: %v\n%s", err, out)
	}
}

// buildAndStartFlex builds a Docker image from the given Dockerfile name and
// starts a flex consumption container in placeholder mode.
func buildAndStartFlex(t testing.TB, dockerfileName, imageName string) *flexContainer {
	t.Helper()

	dockerfile := filepath.Join(repoRoot(), "tests", "consumption", dockerfileName)
	out, err := exec.Command("docker", "build", "-t", imageName, "-f", dockerfile, repoRoot()).CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build %s: %v\n%s", imageName, err, out)
	}
	t.Cleanup(func() {
		exec.Command("docker", "rmi", imageName).Run()
	})

	id := fmt.Sprintf("goworker-test-%d", time.Now().UnixNano())
	args := []string{
		"run", "-p", "0:80", "-d",
		"--name", id, "--privileged",
		"--cap-add", "SYS_ADMIN",
		"--device", "/dev/fuse",
		"-e", fmt.Sprintf("CONTAINER_ENCRYPTION_KEY=%s", dummyContKey),
		"-e", "WEBSITE_PLACEHOLDER_MODE=1",
		"-e", fmt.Sprintf("WEBSITE_SITE_NAME=%s", id),
		"-e", fmt.Sprintf("WEBSITE_POD_NAME=%s", id),
		"-e", "WEBSITE_SKU=FlexConsumption",
		imageName,
	}

	out, err = exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("failed to start container: %v\n%s", err, out)
	}

	fc := &flexContainer{id: id, t: t}
	t.Cleanup(func() { fc.kill() })

	time.Sleep(3 * time.Second)

	portOut, err := exec.Command("docker", "port", id).CombinedOutput()
	if err != nil {
		t.Fatalf("failed to get container port: %v\n%s", err, portOut)
	}
	parts := strings.Split(strings.TrimSpace(string(portOut)), ":")
	fc.port = parts[len(parts)-1]

	time.Sleep(6 * time.Second)
	return fc
}

// startFlexContainer starts a container using Dockerfile.flex-test (Option A:
// user provides worker.config.json in deploy package).
func startFlexContainer(t testing.TB) *flexContainer {
	return buildAndStartFlex(t, "Dockerfile.flex-test", flexTestImage)
}

// startFlexContainerIdeal starts a container using Dockerfile.flex-test-ideal
// (worker.config.json baked in image, FUNCTIONS_WORKER_RUNTIME=native set).
// This demonstrates the host limitation: the host fatally fails during
// placeholder because it tries to start the worker binary that doesn't exist.
func startFlexContainerIdeal(t testing.TB) *flexContainer {
	return buildAndStartFlex(t, "Dockerfile.flex-test-ideal", flexTestIdealImage)
}

// startFlexContainerIdealWorkaround starts a container using
// Dockerfile.flex-test-ideal-workaround (worker.config.json baked in image,
// FUNCTIONS_WORKER_RUNTIME intentionally NOT set so the host skips worker init
// during placeholder).
func startFlexContainerIdealWorkaround(t testing.TB) *flexContainer {
	return buildAndStartFlex(t, "Dockerfile.flex-test-ideal-workaround", flexTestIdealWAImage)
}

// startFlexContainerHostFix starts a container using
// Dockerfile.flex-test-ideal-hostfix which includes a modified host with
// skipPlaceholderInit support. Requires CUSTOM_HOST_PATH env var pointing to the
// self-contained host publish output (e.g. azure-functions-host/out/publish/linux-x64).
func startFlexContainerHostFix(t testing.TB) *flexContainer {
	t.Helper()

	hostPath := os.Getenv("CUSTOM_HOST_PATH")
	if hostPath == "" {
		t.Skip("CUSTOM_HOST_PATH not set -- skipping hostfix test (requires a custom host build)")
	}

	dockerfile := filepath.Join(repoRoot(), "tests", "consumption", "Dockerfile.flex-test-ideal-hostfix")
	out, err := exec.Command("docker", "build", "-t", flexTestHostFixImage, "-f", dockerfile, hostPath).CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build %s: %v\n%s", flexTestHostFixImage, err, out)
	}
	t.Cleanup(func() {
		exec.Command("docker", "rmi", flexTestHostFixImage).Run()
	})

	id := fmt.Sprintf("goworker-test-%d", time.Now().UnixNano())
	args := []string{
		"run", "-p", "0:80", "-d",
		"--name", id, "--privileged",
		"--cap-add", "SYS_ADMIN",
		"--device", "/dev/fuse",
		"-e", fmt.Sprintf("CONTAINER_ENCRYPTION_KEY=%s", dummyContKey),
		"-e", "WEBSITE_PLACEHOLDER_MODE=1",
		"-e", fmt.Sprintf("WEBSITE_SITE_NAME=%s", id),
		"-e", fmt.Sprintf("WEBSITE_POD_NAME=%s", id),
		"-e", "WEBSITE_SKU=FlexConsumption",
		flexTestHostFixImage,
	}

	out, err = exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("failed to start container: %v\n%s", err, out)
	}

	fc := &flexContainer{id: id, t: t}
	t.Cleanup(func() { fc.kill() })

	time.Sleep(3 * time.Second)

	portOut, err := exec.Command("docker", "port", id).CombinedOutput()
	if err != nil {
		t.Fatalf("failed to get container port: %v\n%s", err, portOut)
	}
	parts := strings.Split(strings.TrimSpace(string(portOut)), ":")
	fc.port = parts[len(parts)-1]

	time.Sleep(6 * time.Second)
	return fc
}

func (fc *flexContainer) url() string {
	return fmt.Sprintf("http://localhost:%s", fc.port)
}

func (fc *flexContainer) kill() {
	exec.Command("docker", "rm", "-f", fc.id).Run()
}

func (fc *flexContainer) logs() string {
	out, _ := exec.Command("docker", "logs", fc.id).CombinedOutput()
	return string(out)
}

// waitForPing polls /admin/host/ping until it returns 200.
func (fc *flexContainer) waitForPing(timeout time.Duration) {
	fc.t.Helper()
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 5 * time.Second}
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest("GET", fc.url()+"/admin/host/ping", nil)
		fc.addAuthHeaders(req)
		if resp, err := client.Do(req); err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return
		}
		time.Sleep(1 * time.Second)
	}
	fc.t.Fatalf("container did not become ready within %v\nlogs:\n%s", timeout, fc.logs())
}

// killWorkerProcess kills the worker process (/home/site/wwwroot/app) inside
// the container, leaving the host running but with a dead worker channel.
func (fc *flexContainer) killWorkerProcess() {
	fc.t.Helper()
	// pkill by the executable path
	exec.Command("docker", "exec", fc.id, "pkill", "-f", "/home/site/wwwroot/app").Run()
}

// deployApp extracts a zip and copies its contents into /home/site/wwwroot/.
func (fc *flexContainer) deployApp(zipPath string) {
	fc.t.Helper()

	tmpDir := fc.t.TempDir()
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		fc.t.Fatalf("failed to open zip: %v", err)
	}
	defer zr.Close()
	for _, f := range zr.File {
		dst := filepath.Join(tmpDir, f.Name)
		if f.FileInfo().IsDir() {
			os.MkdirAll(dst, 0o755)
			continue
		}
		rc, _ := f.Open()
		data, _ := io.ReadAll(rc)
		rc.Close()
		os.WriteFile(dst, data, f.Mode())
	}

	exec.Command("docker", "exec", fc.id, "mkdir", "-p", "/home/site/wwwroot").Run()

	entries, _ := os.ReadDir(tmpDir)
	for _, e := range entries {
		src := filepath.Join(tmpDir, e.Name())
		out, err := exec.Command("docker", "cp", src, fc.id+":/home/site/wwwroot/"+e.Name()).CombinedOutput()
		if err != nil {
			fc.t.Fatalf("failed to copy %s to container: %v\n%s", e.Name(), err, out)
		}
	}

	out, err := exec.Command("docker", "exec", fc.id, "chmod", "+x", "/home/site/wwwroot/app").CombinedOutput()
	if err != nil {
		fc.t.Fatalf("failed to chmod app: %v\n%s", err, out)
	}
}

// specialize sends /admin/instance/assign to specialize the container.
func (fc *flexContainer) specialize(env map[string]string) {
	fc.t.Helper()

	env["FUNCTIONS_EXTENSION_VERSION"] = "~4"
	env["WEBSITE_SITE_NAME"] = fc.id
	env["WEBSITE_POD_NAME"] = fc.id

	encCtx := encryptContext(dummyContKey, fc.id, env)
	body, _ := json.Marshal(map[string]string{"encryptedContext": encCtx})

	req, _ := http.NewRequest("POST", fc.url()+"/admin/instance/assign", bytes.NewReader(body))
	fc.addAuthHeaders(req)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fc.t.Fatalf("assign request failed: %v\nlogs:\n%s", err, fc.logs())
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		respBody, _ := io.ReadAll(resp.Body)
		fc.t.Fatalf("assign returned %d: %s\nlogs:\n%s", resp.StatusCode, respBody, fc.logs())
	}
}

// sendRequest sends an HTTP request with auth headers, retrying until 200.
func (fc *flexContainer) sendRequest(method, path string) (int, []byte) {
	fc.t.Helper()
	client := &http.Client{Timeout: 10 * time.Second}

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(method, fc.url()+path, nil)
		fc.addAuthHeaders(req)
		resp, err := client.Do(req)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 200 {
			return resp.StatusCode, body
		}
		time.Sleep(2 * time.Second)
	}
	fc.t.Fatalf("request %s %s did not return 200 within timeout\nlogs:\n%s", method, path, fc.logs())
	return 0, nil
}

func (fc *flexContainer) addAuthHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+generateJWT(fc.id))
	req.Header.Set("x-site-deployment-id", fc.id)
	req.Header.Set("x-ms-client-request-id", fmt.Sprintf("%d", time.Now().UnixNano()))
	req.Header.Set("x-ms-request-id", fmt.Sprintf("%d", time.Now().UnixNano()))
}

// buildSampleZip cross-compiles a sample app for linux/amd64 and creates a zip
// containing the binary (named "app"), host.json, and worker.config.json.
func buildSampleZip(t testing.TB, sampleName string) string {
	t.Helper()

	sampleDir := filepath.Join(samplesDir(), sampleName)
	tmpDir := t.TempDir()
	binaryPath := filepath.Join(tmpDir, "app")

	// Cross-compile for linux/amd64
	cmd := exec.Command("go", "build", "-o", binaryPath, ".")
	cmd.Dir = sampleDir
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cross-compile failed for %s:\n%s", sampleName, out)
	}

	// Write worker.config.json — deployed alongside the app binary.
	// defaultExecutablePath must be absolute because .NET's Process.Start
	// resolves relative filenames against the calling process's CWD, not the
	// child's WorkingDirectory.
	workerConfig := `{
    "description": {
        "language": "native",
        "supportedOperatingSystems": ["LINUX"],
        "supportedArchitectures": ["X64", "Arm64"],
        "defaultExecutablePath": "/home/site/wwwroot/app",
        "executableWorkingDirectory": "/home/site/wwwroot",
        "workerIndexing": "true"
    },
    "processOptions": {
        "initializationTimeout": "00:02:00",
        "environmentReloadTimeout": "00:02:00"
    }
}`
	workerConfigPath := filepath.Join(tmpDir, "worker.config.json")
	os.WriteFile(workerConfigPath, []byte(workerConfig), 0o644)

	// Create zip
	zipPath := filepath.Join(tmpDir, sampleName+".zip")
	zipFile, err := os.Create(zipPath)
	if err != nil {
		t.Fatalf("failed to create zip: %v", err)
	}
	defer zipFile.Close()

	w := zip.NewWriter(zipFile)
	addFileToZip(t, w, binaryPath, "app")
	hostJSON := filepath.Join(sampleDir, "host.json")
	if _, err := os.Stat(hostJSON); err == nil {
		addFileToZip(t, w, hostJSON, "host.json")
	}
	addFileToZip(t, w, workerConfigPath, "worker.config.json")
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close zip: %v", err)
	}

	return zipPath
}

// buildSampleZipMinimal cross-compiles a sample app for linux/amd64 and creates
// a zip containing only the binary (named "app") and host.json — no
// worker.config.json. Used with the ideal image where the config is in the image.
func buildSampleZipMinimal(t testing.TB, sampleName string) string {
	t.Helper()

	sampleDir := filepath.Join(samplesDir(), sampleName)
	tmpDir := t.TempDir()
	binaryPath := filepath.Join(tmpDir, "app")

	cmd := exec.Command("go", "build", "-o", binaryPath, ".")
	cmd.Dir = sampleDir
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cross-compile failed for %s:\n%s", sampleName, out)
	}

	zipPath := filepath.Join(tmpDir, sampleName+".zip")
	zipFile, err := os.Create(zipPath)
	if err != nil {
		t.Fatalf("failed to create zip: %v", err)
	}
	defer zipFile.Close()

	w := zip.NewWriter(zipFile)
	addFileToZip(t, w, binaryPath, "app")
	hostJSON := filepath.Join(sampleDir, "host.json")
	if _, err := os.Stat(hostJSON); err == nil {
		addFileToZip(t, w, hostJSON, "host.json")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close zip: %v", err)
	}

	return zipPath
}

func addFileToZip(t testing.TB, w *zip.Writer, srcPath, name string) {
	t.Helper()
	data, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", srcPath, err)
	}
	f, err := w.Create(name)
	if err != nil {
		t.Fatalf("failed to add %s to zip: %v", name, err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatalf("failed to write %s to zip: %v", name, err)
	}
}

// --- Crypto helpers (AES-CBC encryption + JWT for specialization auth) ---

func generateJWT(siteID string) string {
	keyBytes, _ := base64.StdEncoding.DecodeString(dummyContKey)
	signingKey := keyBytes[:32]

	now := time.Now()
	claims := jwt.MapClaims{
		"exp": now.Add(24 * time.Hour).Unix(),
		"iat": now.Unix(),
		"nbf": now.Unix(),
		"iss": fmt.Sprintf("https://%s.azurewebsites.net", siteID),
		"aud": siteID,
		"sub": siteID,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, _ := token.SignedString(signingKey)
	return signed
}

func encryptContext(encKey, siteName string, env map[string]string) string {
	env["WEBSITE_SITE_NAME"] = siteName
	ctx := map[string]interface{}{
		"siteId":      1,
		"siteName":    siteName,
		"environment": env,
	}
	plainText, _ := json.Marshal(ctx)

	keyBytes, _ := base64.StdEncoding.DecodeString(encKey)
	aesKey := keyBytes[:32]

	blockSize := aes.BlockSize
	padLen := blockSize - (len(plainText) % blockSize)
	padded := append(plainText, bytes.Repeat([]byte{byte(padLen)}, padLen)...)

	iv := []byte(encryptionIV)
	block, _ := aes.NewCipher(aesKey)
	mode := cipher.NewCBCEncrypter(block, iv)
	encrypted := make([]byte, len(padded))
	mode.CryptBlocks(encrypted, padded)

	hash := sha256.Sum256(aesKey)

	ivB64 := base64.StdEncoding.EncodeToString(iv)
	encB64 := base64.StdEncoding.EncodeToString(encrypted)
	keyHashB64 := base64.StdEncoding.EncodeToString(hash[:])

	return fmt.Sprintf("%s.%s.%s", ivB64, encB64, keyHashB64)
}
