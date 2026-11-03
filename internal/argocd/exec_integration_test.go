package argocd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

// fakeTerminalServer emulates Argo CD's /terminal endpoint: it enforces the auth
// cookie and required query parameters, then runs the injected stdin lines with a
// local POSIX shell - validating the full client protocol over a real websocket.
func fakeTerminalServer(t *testing.T) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(authCookieName)
		if err != nil || cookie.Value != "test-token" {
			http.Error(w, "Auth cookie not found", http.StatusBadRequest)
			return
		}
		q := r.URL.Query()
		for _, param := range []string{"pod", "container", "appName", "projectName", "namespace", "appNamespace"} {
			if q.Get(param) == "" {
				http.Error(w, "Missing required parameters", http.StatusBadRequest)
				return
			}
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		var script string
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var msg terminalMessage
			if err := json.Unmarshal(raw, &msg); err != nil {
				return
			}
			if msg.Operation != "stdin" {
				continue
			}
			// Act as the remote shell: accumulate stdin lines and run them as one
			// script once the client closes the session with exit, streaming the
			// combined output (including the EXIT trap marker) back as frames.
			script += msg.Data
			if strings.TrimSpace(msg.Data) != "exit" {
				continue
			}
			cmd := exec.Command("sh", "-c", script)
			output, _ := cmd.CombinedOutput()
			frame, _ := json.Marshal(terminalMessage{Operation: "stdout", Data: string(output)})
			if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
				return
			}
			return
		}
	}))
}

func TestExecEndToEndAgainstFakeTerminalServer(t *testing.T) {
	server := fakeTerminalServer(t)
	defer server.Close()

	client := execTestClient(server.URL, true)
	session, err := client.StartExecSession(context.Background(), execTestResolved(), ExecOptions{
		PodName:   "web-1",
		Container: "app",
	})
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	defer func() { _ = session.Close() }()

	var out strings.Builder
	code, err := session.Run(context.Background(), "printf 'hello '; echo world; exit 5", &out)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if code != 5 {
		t.Errorf("expected remote exit code 5, got %d", code)
	}
	if out.String() != "hello world\n" {
		t.Errorf("expected only command output, got %q", out.String())
	}
}

func TestExecHandshakeErrorSurfacesServerMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Pod not running", http.StatusBadRequest)
	}))
	defer server.Close()

	client := execTestClient(server.URL, true)
	_, err := client.StartExecSession(context.Background(), execTestResolved(), ExecOptions{
		PodName:   "web-1",
		Container: "app",
	})
	if err == nil || !strings.Contains(err.Error(), "Pod not running") || !strings.Contains(err.Error(), "400") {
		t.Fatalf("expected server rejection detail, got %v", err)
	}
}
