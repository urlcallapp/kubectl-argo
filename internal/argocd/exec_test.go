package argocd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"kubectl-argo/internal/config"
)

func execTestClient(serverAddr string, plainText bool) *Client {
	return &Client{cfg: &config.ArgoConfig{ServerAddr: serverAddr, AuthToken: "test-token", PlainText: plainText}}
}

func execTestResolved() *ResolvedApp {
	return &ResolvedApp{
		AppName:         "prod-payments",
		AppNamespace:    "argocd",
		TargetNamespace: "payments-prod",
		Project:         "prod",
	}
}

// fakeWSConn implements wsConn, replaying queued server frames and recording writes.
type fakeWSConn struct {
	incoming [][]byte
	readErr  error
	writes   [][]byte
	closed   bool
}

func (f *fakeWSConn) ReadMessage() (int, []byte, error) {
	if len(f.incoming) == 0 {
		if f.readErr != nil {
			return 0, nil, f.readErr
		}
		return 0, nil, fmt.Errorf("fake: no more frames")
	}
	frame := f.incoming[0]
	f.incoming = f.incoming[1:]
	return 1, frame, nil // websocket.TextMessage
}

func (f *fakeWSConn) WriteMessage(_ int, data []byte) error {
	f.writes = append(f.writes, append([]byte(nil), data...))
	return nil
}

func (f *fakeWSConn) Close() error {
	f.closed = true
	return nil
}

func stdoutFrame(t *testing.T, data string) []byte {
	t.Helper()
	frame, err := json.Marshal(terminalMessage{Operation: "stdout", Data: data})
	if err != nil {
		t.Fatalf("marshal frame: %v", err)
	}
	return frame
}

func (f *fakeWSConn) sentMessages(t *testing.T) []terminalMessage {
	t.Helper()
	msgs := make([]terminalMessage, 0, len(f.writes))
	for _, w := range f.writes {
		var m terminalMessage
		if err := json.Unmarshal(w, &m); err != nil {
			t.Fatalf("recorded write is not a terminal message: %v", err)
		}
		msgs = append(msgs, m)
	}
	return msgs
}

func TestBuildTerminalURL(t *testing.T) {
	client := execTestClient("argocd.example.com", false)
	rawURL, err := client.buildTerminalURL(execTestResolved(), ExecOptions{PodName: "web-1", Container: "app"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("unparseable URL %q: %v", rawURL, err)
	}
	if parsed.Scheme != "wss" {
		t.Errorf("expected wss scheme, got %q", parsed.Scheme)
	}
	if parsed.Host != "argocd.example.com" || parsed.Path != "/terminal" {
		t.Errorf("unexpected host/path: %s%s", parsed.Host, parsed.Path)
	}
	q := parsed.Query()
	want := map[string]string{
		"pod":          "web-1",
		"container":    "app",
		"appName":      "prod-payments",
		"projectName":  "prod",
		"namespace":    "payments-prod",
		"appNamespace": "argocd",
	}
	for key, val := range want {
		if q.Get(key) != val {
			t.Errorf("query %s: expected %q, got %q", key, val, q.Get(key))
		}
	}
	if q.Get("shell") != "" {
		t.Errorf("shell param should be omitted when unset, got %q", q.Get("shell"))
	}
}

func TestBuildTerminalURLPlaintextAndShell(t *testing.T) {
	client := execTestClient("http://localhost:8080", true)
	resolved := execTestResolved()
	resolved.Project = ""
	resolved.AppNamespace = ""
	rawURL, err := client.buildTerminalURL(resolved, ExecOptions{PodName: "web-1", Container: "app", Shell: "sh"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	parsed, _ := url.Parse(rawURL)
	if parsed.Scheme != "ws" {
		t.Errorf("expected ws scheme for plaintext, got %q", parsed.Scheme)
	}
	q := parsed.Query()
	if q.Get("projectName") != "default" {
		t.Errorf("empty project should default to %q, got %q", "default", q.Get("projectName"))
	}
	if q.Get("appNamespace") != "argocd" {
		t.Errorf("empty app namespace should fall back to %q, got %q", "argocd", q.Get("appNamespace"))
	}
	if q.Get("shell") != "sh" {
		t.Errorf("expected shell=sh, got %q", q.Get("shell"))
	}
}

func TestExecSessionSendAndResize(t *testing.T) {
	conn := &fakeWSConn{}
	session := &ExecSession{conn: conn}

	if err := session.Send("ls\n", 120, 40); err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := session.Resize(100, 30); err != nil {
		t.Fatalf("resize: %v", err)
	}

	msgs := conn.sentMessages(t)
	if len(msgs) != 2 {
		t.Fatalf("expected 2 frames, got %d", len(msgs))
	}
	if msgs[0].Operation != "stdin" || msgs[0].Data != "ls\n" || msgs[0].Cols != 120 || msgs[0].Rows != 40 {
		t.Errorf("unexpected stdin frame: %+v", msgs[0])
	}
	if msgs[1].Operation != "resize" || msgs[1].Cols != 100 || msgs[1].Rows != 30 {
		t.Errorf("unexpected resize frame: %+v", msgs[1])
	}
}

func TestStreamRoutesStdoutAndStopsOnReconnectCommand(t *testing.T) {
	var out strings.Builder
	conn := &fakeWSConn{
		incoming: [][]byte{
			stdoutFrame(t, "hello\n"),
			[]byte(`{"Code":1}`),
		},
	}
	session := &ExecSession{conn: conn}

	err := session.Stream(context.Background(), func(data string) { out.WriteString(data) })
	if err == nil || !strings.Contains(err.Error(), "refreshed the session token") {
		t.Fatalf("expected reconnect error, got %v", err)
	}
	if out.String() != "hello\n" {
		t.Errorf("expected stdout before reconnect, got %q", out.String())
	}
}

func TestRunStreamsOutputAndReturnsExitCode(t *testing.T) {
	// stty worked: only the stty line is echoed, then command output, then marker.
	conn := &fakeWSConn{
		incoming: [][]byte{
			stdoutFrame(t, "stty -echo 2>/dev/null\r\n"),
			stdoutFrame(t, "total 8\n-rw-r--r-- 1 root root 42 app.conf\n"),
			stdoutFrame(t, "\n"+execDoneMarker+":0\n"),
		},
		readErr: errFakeEOF,
	}
	session := &ExecSession{conn: conn}

	var out strings.Builder
	code, err := session.Run(context.Background(), "ls -l /etc", &out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}
	if out.String() != "total 8\n-rw-r--r-- 1 root root 42 app.conf\n" {
		t.Errorf("unexpected output:\n%q", out.String())
	}

	// The remote shell must receive: stty, the EXIT trap with the marker printf,
	// the command line itself, and the final exit.
	msgs := conn.sentMessages(t)
	if len(msgs) != 4 {
		t.Fatalf("expected 4 injected lines, got %d", len(msgs))
	}
	if msgs[0].Data != "stty -echo 2>/dev/null\n" {
		t.Errorf("unexpected first injected line: %q", msgs[0].Data)
	}
	for _, want := range []string{"trap", "printf", execDoneMarker, "EXIT"} {
		if !strings.Contains(msgs[1].Data, want) {
			t.Errorf("trap line missing %q: %q", want, msgs[1].Data)
		}
	}
	if msgs[2].Data != "ls -l /etc\n" {
		t.Errorf("unexpected command line: %q", msgs[2].Data)
	}
	if msgs[3].Data != "exit\n" {
		t.Errorf("unexpected final injected line: %q", msgs[3].Data)
	}
}

func TestRunStripsEchoWhenSttyUnavailable(t *testing.T) {
	// Minimal image without stty: both injected lines echo back, plus the error.
	sttyLine := "stty -echo 2>/dev/null"
	command := "echo hello"
	trapLine := `trap '__ka_rc=$?; printf "\n` + execDoneMarker + `:%s\n" "$__ka_rc"' EXIT`
	conn := &fakeWSConn{
		incoming: [][]byte{
			stdoutFrame(t, sttyLine+"\r\n"),
			stdoutFrame(t, trapLine+"\r\n"),
			stdoutFrame(t, command+"\r\n"),
			stdoutFrame(t, "exit\r\n"),
			stdoutFrame(t, "hello\n"),
			stdoutFrame(t, "\n"+execDoneMarker+":7\n"),
		},
		readErr: errFakeEOF,
	}
	session := &ExecSession{conn: conn}

	var out strings.Builder
	code, err := session.Run(context.Background(), command, &out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != 7 {
		t.Errorf("expected exit code 7, got %d", code)
	}
	if out.String() != "hello\n" {
		t.Errorf("expected only command output, got %q", out.String())
	}
}

func TestRunPropagatesNonZeroExitCode(t *testing.T) {
	conn := &fakeWSConn{
		incoming: [][]byte{
			stdoutFrame(t, "stty -echo 2>/dev/null\r\n"),
			stdoutFrame(t, "ls: /missing: No such file or directory\n"),
			stdoutFrame(t, "\n"+execDoneMarker+":2\n"),
		},
		readErr: errFakeEOF,
	}
	session := &ExecSession{conn: conn}

	var out strings.Builder
	code, err := session.Run(context.Background(), "ls /missing", &out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != 2 {
		t.Errorf("expected exit code 2, got %d", code)
	}
}

func TestRunWithoutMarkerIsAnError(t *testing.T) {
	conn := &fakeWSConn{
		incoming: [][]byte{
			stdoutFrame(t, "stty -echo 2>/dev/null\r\n"),
			stdoutFrame(t, "connection dropped\n"),
		},
		readErr: errFakeEOF,
	}
	session := &ExecSession{conn: conn}

	var out strings.Builder
	if _, err := session.Run(context.Background(), "ls", &out); err == nil {
		t.Fatalf("expected an error when the marker never arrives")
	}
	// Buffered output must still reach the caller.
	if !strings.Contains(out.String(), "connection dropped") {
		t.Errorf("expected buffered output to flush, got %q", out.String())
	}
}

func TestShellQuote(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"ls", "-l", "/etc"}, "ls -l /etc"},
		{[]string{"sh", "-c", "echo hi; exit 0"}, `sh -c 'echo hi; exit 0'`},
		{[]string{"echo", "it's"}, `echo 'it'\''s'`},
		{[]string{"printenv", "POD_NAME"}, "printenv POD_NAME"},
		{[]string{""}, `''`},
	}
	for _, tc := range cases {
		if got := ShellQuote(tc.args); got != tc.want {
			t.Errorf("ShellQuote(%v) = %q, want %q", tc.args, got, tc.want)
		}
	}
}

func TestResolvePodContainer(t *testing.T) {
	podManifest := `{
		"metadata": {"name": "web-1", "annotations": {"kubectl.kubernetes.io/default-container": "app"}},
		"spec": {"containers": [{"name": "first"}, {"name": "app"}]}
	}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("missing bearer token on manifest request")
		}
		if r.URL.Query().Get("resourceName") != "web-1" || r.URL.Query().Get("kind") != "Pod" {
			t.Errorf("unexpected manifest query: %s", r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"manifest": podManifest})
	}))
	defer server.Close()

	client := execTestClient(server.URL, true)

	// Explicit -c wins without any lookup.
	if name, err := client.ResolvePodContainer(context.Background(), execTestResolved(), "web-1", "explicit"); err != nil || name != "explicit" {
		t.Errorf("explicit container: got %q, %v", name, err)
	}
	// The kubectl default-container annotation is honored.
	if name, err := client.ResolvePodContainer(context.Background(), execTestResolved(), "web-1", ""); err != nil || name != "app" {
		t.Errorf("annotation default: got %q, %v", name, err)
	}
}

func TestResolvePodContainerFallsBackToFirstContainer(t *testing.T) {
	podManifest := `{"metadata": {"name": "web-1"}, "spec": {"containers": [{"name": "only"}, {"name": "sidecar"}]}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"manifest": podManifest})
	}))
	defer server.Close()

	client := execTestClient(server.URL, true)
	if name, err := client.ResolvePodContainer(context.Background(), execTestResolved(), "web-1", ""); err != nil || name != "only" {
		t.Errorf("first-container fallback: got %q, %v", name, err)
	}
}

var errFakeEOF = fmt.Errorf("fake: connection closed")
