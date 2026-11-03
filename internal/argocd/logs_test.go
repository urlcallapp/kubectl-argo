package argocd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"kubectl-argo/internal/config"
)

func logsTestClient(serverAddr string) *Client {
	return &Client{cfg: &config.ArgoConfig{ServerAddr: serverAddr, AuthToken: "test-token", PlainText: true}}
}

func logsTestResolved() *ResolvedApp {
	return &ResolvedApp{AppName: "prod-payments", AppNamespace: "argocd", TargetNamespace: "payments-prod"}
}

// restLogServer replays the given raw response body for the PodLogs REST endpoint.
func restLogServer(t *testing.T, status int, body string, check func(r *http.Request)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/api/v1/applications/prod-payments/pods/web-1/logs") {
			t.Errorf("unexpected logs path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("missing bearer token")
		}
		if check != nil {
			check(r)
		}
		w.WriteHeader(status)
		_, _ = fmt.Fprint(w, body)
	}))
}

func TestStreamPodLogsRESTJSONLines(t *testing.T) {
	lines := []restLogStreamEnvelope{}
	for _, content := range []string{"first", "second"} {
		var env restLogStreamEnvelope
		env.Result.Content = content
		lines = append(lines, env)
	}
	var last restLogStreamEnvelope
	last.Result.Content = "third"
	last.Result.Last = true
	lines = append(lines, last)

	var body strings.Builder
	for _, env := range lines {
		raw, _ := json.Marshal(env)
		body.WriteString(string(raw) + "\n")
	}

	server := restLogServer(t, http.StatusOK, body.String(), func(r *http.Request) {
		q := r.URL.Query()
		if q.Get("namespace") != "payments-prod" || q.Get("container") != "app" || q.Get("tailLines") != "50" {
			t.Errorf("unexpected logs query: %s", r.URL.RawQuery)
		}
	})
	defer server.Close()

	var out strings.Builder
	opts := PodLogsOptions{PodName: "web-1", Container: "app", TailLines: 50}
	err := logsTestClient(server.URL).streamPodLogsREST(context.Background(), logsTestResolved(), opts, &out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.String() != "first\nsecond\nthird\n" {
		t.Errorf("unexpected output: %q", out.String())
	}
}

func TestStreamPodLogsRESTTimestamps(t *testing.T) {
	var env restLogStreamEnvelope
	env.Result.Content = "hello"
	env.Result.TimeStampStr = "2026-11-03T08:00:00Z"
	raw, _ := json.Marshal(env)

	server := restLogServer(t, http.StatusOK, string(raw)+"\n", nil)
	defer server.Close()

	var out strings.Builder
	opts := PodLogsOptions{PodName: "web-1", Timestamps: true}
	if err := logsTestClient(server.URL).streamPodLogsREST(context.Background(), logsTestResolved(), opts, &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.String() != "2026-11-03T08:00:00Z hello\n" {
		t.Errorf("timestamp prefix missing: %q", out.String())
	}
}

func TestStreamPodLogsRESTErrorEnvelope(t *testing.T) {
	var env restLogStreamEnvelope
	env.Error.Message = "container \"app\" in pod \"web-1\" is waiting to start"
	raw, _ := json.Marshal(env)

	server := restLogServer(t, http.StatusOK, string(raw)+"\n", nil)
	defer server.Close()

	var out strings.Builder
	opts := PodLogsOptions{PodName: "web-1"}
	err := logsTestClient(server.URL).streamPodLogsREST(context.Background(), logsTestResolved(), opts, &out)
	if err == nil || !strings.Contains(err.Error(), "waiting to start") {
		t.Fatalf("expected the server's error message, got %v", err)
	}
}

func TestStreamPodLogsRESTHTTPError(t *testing.T) {
	server := restLogServer(t, http.StatusForbidden, `{"error":"permission denied"}`, nil)
	defer server.Close()

	var out strings.Builder
	opts := PodLogsOptions{PodName: "web-1"}
	err := logsTestClient(server.URL).streamPodLogsREST(context.Background(), logsTestResolved(), opts, &out)
	if err == nil {
		t.Fatalf("expected an error for HTTP 403")
	}
	for _, want := range []string{"403", "permission denied"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}

func TestStreamPodLogsRESTPlainTextFallback(t *testing.T) {
	server := restLogServer(t, http.StatusOK, "plain log line\nanother\n", nil)
	defer server.Close()

	var out strings.Builder
	opts := PodLogsOptions{PodName: "web-1"}
	if err := logsTestClient(server.URL).streamPodLogsREST(context.Background(), logsTestResolved(), opts, &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.String() != "plain log line\nanother\n" {
		t.Errorf("plain text should pass through, got %q", out.String())
	}
}

// StreamPodLogs must fall back to the REST endpoint when the gRPC stream cannot
// be established (here: the server only speaks plain HTTP).
func TestStreamPodLogsFallsBackToREST(t *testing.T) {
	var env restLogStreamEnvelope
	env.Result.Content = "via-rest"
	env.Result.Last = true
	raw, _ := json.Marshal(env)

	server := restLogServer(t, http.StatusOK, string(raw)+"\n", nil)
	defer server.Close()

	cfg := &config.ArgoConfig{ServerAddr: server.URL, AuthToken: "test-token", PlainText: true}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("client init: %v", err)
	}

	var out strings.Builder
	opts := PodLogsOptions{PodName: "web-1"}
	if err := client.StreamPodLogs(context.Background(), logsTestResolved(), opts, &out); err != nil {
		t.Fatalf("expected REST fallback to succeed, got %v", err)
	}
	if out.String() != "via-rest\n" {
		t.Errorf("unexpected output: %q", out.String())
	}
}
