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
	dummyContKey = "MDEyMzQ1Njc4OUFCQ0RFRjAxMjM0NTY3ODlBQkNERUY="
	encryptionIV = "0123456789abcedf"
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
	return buildAndStartFlexWithOpts(t, dockerfileName, imageName, true, nil)
}

// buildAndStartFlexSpecialized builds and starts a container that is already
// past specialization (WEBSITE_PLACEHOLDER_MODE=0). This matches the production
// scenario where the host restarts after a previous specialization.
func buildAndStartFlexSpecialized(t testing.TB, dockerfileName, imageName string, extraEnv map[string]string) *flexContainer {
	return buildAndStartFlexWithOpts(t, dockerfileName, imageName, false, extraEnv)
}

// buildAndStartFlexWithOpts is the shared implementation for starting containers.
func buildAndStartFlexWithOpts(t testing.TB, dockerfileName, imageName string, placeholderMode bool, extraEnv map[string]string) *flexContainer {
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
		"-e", fmt.Sprintf("WEBSITE_SITE_NAME=%s", id),
		"-e", fmt.Sprintf("WEBSITE_POD_NAME=%s", id),
		"-e", "WEBSITE_SKU=FlexConsumption",
	}
	if placeholderMode {
		args = append(args, "-e", "WEBSITE_PLACEHOLDER_MODE=1")
	} else {
		args = append(args, "-e", "WEBSITE_PLACEHOLDER_MODE=0")
	}
	for k, v := range extraEnv {
		args = append(args, "-e", fmt.Sprintf("%s=%s", k, v))
	}
	args = append(args, imageName)

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
	// Kill the proxy process. The proxy's cmd.Wait() goroutine detects child
	// death and exits, or killing the proxy directly kills both.
	// On restart, the proxy finds /home/site/wwwroot/app and execs into it.
	exec.Command("docker", "exec", fc.id, "pkill", "-9", "-f", "workers/native/proxy").Run()
}

// restartHost calls POST /admin/host/restart to trigger a script host rebuild.
// This causes the host to create new worker channels, which in turn starts
// a new proxy process that can pick up a newly deployed app binary.
func (fc *flexContainer) restartHost() {
	fc.t.Helper()
	req, _ := http.NewRequest("POST", fc.url()+"/admin/host/restart", nil)
	fc.addAuthHeaders(req)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fc.t.Logf("restart request failed (may be expected if host is rebuilding): %v", err)
		return
	}
	defer resp.Body.Close()
	fc.t.Logf("restart returned %d", resp.StatusCode)
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
	return fc.sendRequestWithTimeout(method, path, 60*time.Second)
}

// sendRequestWithTimeout sends an HTTP request with auth headers, retrying
// until 200 or the given timeout expires.
func (fc *flexContainer) sendRequestWithTimeout(method, path string, timeout time.Duration) (int, []byte) {
	fc.t.Helper()
	client := &http.Client{Timeout: 10 * time.Second}

	deadline := time.Now().Add(timeout)
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
	fc.t.Fatalf("request %s %s did not return 200 within %v\nlogs:\n%s", method, path, timeout, fc.logs())
	return 0, nil
}

func (fc *flexContainer) addAuthHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+generateJWT(fc.id))
	req.Header.Set("x-site-deployment-id", fc.id)
	req.Header.Set("x-ms-client-request-id", fmt.Sprintf("%d", time.Now().UnixNano()))
	req.Header.Set("x-ms-request-id", fmt.Sprintf("%d", time.Now().UnixNano()))
}

// buildSampleZipMinimal cross-compiles a sample app for linux/amd64 and creates
// a zip containing the binary (named "app") and host.json.
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
