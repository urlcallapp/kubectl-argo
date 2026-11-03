package argocd

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// authCookieName is the cookie Argo CD's /terminal endpoint authenticates with.
// The server's util_session.WithAuthMiddleware only accepts the auth cookie,
// not the Authorization header used by the REST/gRPC APIs.
const authCookieName = "argocd.token"

// execReconnectCode is sent by the server as a TerminalCommand when it refreshes
// the session token and expects the client to reconnect with the new token.
// kubectl-argo tokens are static, so this is surfaced as an error instead.
const execReconnectCode = 1

// execDoneMarker terminates the remote command output; the remote shell prints it
// with the command's exit code (see ExecSession.Run).
const execDoneMarker = "__KUBECTL_ARGO_EXEC_DONE__"

// ExecOptions configures a terminal exec session against an Argo CD application.
type ExecOptions struct {
	PodName   string
	Container string
	// Shell optionally requests a specific shell; the Argo CD server only honors
	// shells from its exec.shells allow-list and otherwise tries that list in order.
	Shell string
}

// terminalMessage mirrors the server/application.TerminalMessage websocket protocol.
type terminalMessage struct {
	Operation string `json:"operation"`
	Data      string `json:"data,omitempty"`
	Rows      uint16 `json:"rows,omitempty"`
	Cols      uint16 `json:"cols,omitempty"`
}

// terminalCommand mirrors the server-side TerminalCommand (token-refresh reconnect).
type terminalCommand struct {
	Code int `json:"Code"`
}

// wsConn is the subset of *websocket.Conn ExecSession relies on (faked in tests).
type wsConn interface {
	ReadMessage() (messageType int, p []byte, err error)
	WriteMessage(messageType int, data []byte) error
	Close() error
}

// ExecSession is an open Argo CD terminal websocket session. The remote end is
// always a TTY shell from the server's exec.shells allow-list (default: bash, sh).
type ExecSession struct {
	conn wsConn
}

// StartExecSession opens the Argo CD terminal websocket (GET /terminal, upgraded)
// for the resolved application. Handshake failures (exec feature disabled, pod not
// running, unknown container, RBAC denial) surface the server's HTTP error body.
func (c *Client) StartExecSession(ctx context.Context, resolved *ResolvedApp, opts ExecOptions) (*ExecSession, error) {
	if resolved == nil || resolved.AppName == "" {
		return nil, fmt.Errorf("resolved Argo CD application cannot be empty")
	}
	if strings.TrimSpace(opts.PodName) == "" {
		return nil, fmt.Errorf("pod name cannot be empty")
	}
	if strings.TrimSpace(opts.Container) == "" {
		return nil, fmt.Errorf("container name cannot be empty")
	}

	wsURL, err := c.buildTerminalURL(resolved, opts)
	if err != nil {
		return nil, err
	}

	dialer := websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 30 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: c.cfg.Insecure, //nolint:gosec // controlled via explicit --insecure / ARGOCD_INSECURE
		},
	}
	header := http.Header{}
	header.Set("Cookie", authCookieName+"="+c.cfg.AuthToken)

	conn, resp, err := dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			detail := strings.TrimSpace(string(body))
			if detail == "" {
				detail = resp.Status
			}
			return nil, fmt.Errorf("Argo CD terminal session rejected (HTTP %d): %s", resp.StatusCode, detail)
		}
		return nil, fmt.Errorf("failed to connect to Argo CD terminal endpoint: %w", err)
	}

	return &ExecSession{conn: conn}, nil
}

// buildTerminalURL renders the wss:// URL for the server /terminal endpoint,
// mirroring the query parameters the Argo CD UI sends.
func (c *Client) buildTerminalURL(resolved *ResolvedApp, opts ExecOptions) (string, error) {
	rawServer := strings.TrimSpace(c.cfg.ServerAddr)
	if !strings.Contains(rawServer, "://") {
		scheme := "wss://"
		if c.cfg.PlainText {
			scheme = "ws://"
		}
		rawServer = scheme + rawServer
	}

	parsed, err := url.Parse(rawServer)
	if err != nil {
		return "", fmt.Errorf("invalid Argo CD server address %q: %w", c.cfg.ServerAddr, err)
	}
	switch parsed.Scheme {
	case "https":
		parsed.Scheme = "wss"
	case "http":
		parsed.Scheme = "ws"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("unsupported Argo CD server scheme %q", parsed.Scheme)
	}

	project := strings.TrimSpace(resolved.Project)
	if project == "" {
		project = "default"
	}
	// The server requires a syntactically valid appNamespace; the API always
	// populates metadata.namespace, so an empty value means a stale cache entry
	// from before the field was recorded - fall back to the default install namespace.
	appNamespace := strings.TrimSpace(resolved.AppNamespace)
	if appNamespace == "" {
		appNamespace = "argocd"
	}

	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/terminal"
	q := parsed.Query()
	q.Set("pod", opts.PodName)
	q.Set("container", opts.Container)
	q.Set("appName", resolved.AppName)
	q.Set("projectName", project)
	q.Set("namespace", resolved.TargetNamespace)
	q.Set("appNamespace", appNamespace)
	if shell := strings.TrimSpace(opts.Shell); shell != "" {
		q.Set("shell", shell)
	}
	parsed.RawQuery = q.Encode()

	return parsed.String(), nil
}

// Send writes data to the remote shell's stdin, reporting the terminal size the
// way the Argo CD UI does on every keystroke.
func (s *ExecSession) Send(data string, cols, rows uint16) error {
	frame, err := json.Marshal(terminalMessage{Operation: "stdin", Data: data, Rows: rows, Cols: cols})
	if err != nil {
		return err
	}
	return s.conn.WriteMessage(websocket.TextMessage, frame)
}

// Resize notifies the remote TTY of a terminal size change.
func (s *ExecSession) Resize(cols, rows uint16) error {
	frame, err := json.Marshal(terminalMessage{Operation: "resize", Rows: rows, Cols: cols})
	if err != nil {
		return err
	}
	return s.conn.WriteMessage(websocket.TextMessage, frame)
}

// Close shuts the websocket down; the server then terminates the remote process.
func (s *ExecSession) Close() error {
	return s.conn.Close()
}

// Stream reads server frames until the session ends, calling onStdout with each
// stdout payload. Context cancellation closes the connection and returns nil.
func (s *ExecSession) Stream(ctx context.Context, onStdout func(data string)) error {
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = s.conn.Close()
		case <-done:
		}
	}()

	for {
		messageType, raw, err := s.conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if closeErr, ok := err.(*websocket.CloseError); ok && closeErr.Code == websocket.CloseNormalClosure {
				return nil
			}
			// A closed websocket after the remote process exited is a normal end of session.
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseAbnormalClosure) {
				return fmt.Errorf("terminal session read failed: %w", err)
			}
			return nil
		}
		if messageType != websocket.TextMessage {
			continue
		}

		// The server sends a bare TerminalCommand ({Code: N}) when it refreshes the
		// session token mid-stream; kubectl-argo has no refresh flow, so stop here.
		var cmd terminalCommand
		if err := json.Unmarshal(raw, &cmd); err == nil && cmd.Code != 0 {
			return fmt.Errorf("Argo CD refreshed the session token during exec; please re-run the command")
		}

		var msg terminalMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		if msg.Operation == "stdout" {
			onStdout(msg.Data)
		}
	}
}

// Run executes a non-interactive command line through the remote shell, streams
// its output to out, and returns the remote exit code.
//
// The Argo CD terminal API only offers a TTY shell, so the command is injected
// into the shell and its exit code is reported back through a marker printed by
// a POSIX EXIT trap - the trap fires even when the command line itself calls
// exit. Terminal echo is disabled up front (best effort: minimal images may
// lack stty) and the injected lines and marker are filtered out of the stream.
func (s *ExecSession) Run(ctx context.Context, commandLine string, out io.Writer) (int, error) {
	lines := []string{
		"stty -echo 2>/dev/null",
		`trap '__ka_rc=$?; printf "\n` + execDoneMarker + `:%s\n" "$__ka_rc"' EXIT`,
		commandLine,
		"exit",
	}
	for _, line := range lines {
		if err := s.Send(line+"\n", 0, 0); err != nil {
			return 0, fmt.Errorf("failed to write to terminal session: %w", err)
		}
	}

	filter := newExecOutputFilter(out, lines...)
	var writeErr error
	streamErr := s.Stream(ctx, func(data string) {
		if writeErr == nil {
			writeErr = filter.Write(data)
		}
	})

	flushCode, flushErr := filter.Finalize()
	if writeErr != nil {
		return 0, fmt.Errorf("failed to write command output: %w", writeErr)
	}
	if flushErr != nil {
		return 0, flushErr
	}
	if streamErr != nil {
		return 0, streamErr
	}
	if flushCode < 0 {
		return 0, fmt.Errorf("terminal session ended before the command reported its exit code")
	}
	return flushCode, nil
}

// execOutputFilter strips terminal echo of the injected lines and the exit-code
// marker from the remote output stream. Lines pass through with a one-line delay
// so the marker (and the blank separator line printf emits before it) never leak.
type execOutputFilter struct {
	out       io.Writer
	pending   string   // unterminated tail of the stream
	heldLine  *string  // last complete line, flushed once the next arrives
	echoLines []string // injected lines to strip when echoed back
	exitCode  int      // -1 until the marker arrives
	sawMarker bool
}

func newExecOutputFilter(out io.Writer, echoLines ...string) *execOutputFilter {
	return &execOutputFilter{out: out, echoLines: echoLines, exitCode: -1}
}

func (f *execOutputFilter) Write(data string) error {
	f.pending += data
	for {
		idx := strings.IndexByte(f.pending, '\n')
		if idx < 0 {
			return nil
		}
		line := f.pending[:idx]
		f.pending = f.pending[idx+1:]
		if err := f.handleLine(line); err != nil {
			return err
		}
	}
}

func (f *execOutputFilter) handleLine(line string) error {
	normalized := strings.TrimSuffix(line, "\r")

	// The TTY echoes input until stty takes effect (or forever, when stty is
	// missing); drop echoed copies of the injected lines in order.
	if len(f.echoLines) > 0 && normalized == f.echoLines[0] {
		f.echoLines = f.echoLines[1:]
		return nil
	}

	if code, ok := parseExecDoneMarker(normalized); ok {
		f.exitCode = code
		f.sawMarker = true
		// printf emits "\nMARKER\n": when the command's output already ended with
		// a newline, the held line is the empty separator and is dropped with the marker.
		if f.heldLine != nil && strings.TrimSuffix(*f.heldLine, "\r") == "" {
			f.heldLine = nil
		}
		return f.flushHeld()
	}

	if err := f.flushHeld(); err != nil {
		return err
	}
	held := line
	f.heldLine = &held
	return nil
}

// Finalize flushes any remaining buffered output. It returns the parsed exit code,
// or -1 when the marker never arrived.
func (f *execOutputFilter) Finalize() (int, error) {
	if !f.sawMarker && f.pending != "" {
		if err := f.handleLine(f.pending); err != nil {
			return f.exitCode, err
		}
		f.pending = ""
	}
	if err := f.flushHeld(); err != nil {
		return f.exitCode, err
	}
	return f.exitCode, nil
}

func (f *execOutputFilter) flushHeld() error {
	if f.heldLine == nil {
		return nil
	}
	_, err := io.WriteString(f.out, *f.heldLine+"\n")
	f.heldLine = nil
	return err
}

func parseExecDoneMarker(line string) (int, bool) {
	if !strings.HasPrefix(line, execDoneMarker+":") {
		return 0, false
	}
	code, err := strconv.Atoi(strings.TrimPrefix(line, execDoneMarker+":"))
	if err != nil {
		return 0, false
	}
	return code, true
}

// ShellQuote renders argv as a single POSIX shell command line, so multi-word
// commands survive being injected into the remote shell.
func ShellQuote(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, shellQuoteArg(arg))
	}
	return strings.Join(quoted, " ")
}

func shellQuoteArg(arg string) string {
	if arg != "" && strings.IndexFunc(arg, func(r rune) bool {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return false
		}
		switch r {
		case '_', '-', '.', '/', ':', ',', '=', '%', '@', '+':
			return false
		}
		return true
	}) < 0 {
		return arg
	}
	return "'" + strings.ReplaceAll(arg, "'", `'\''`) + "'"
}

// ResolvePodContainer determines which container exec should target: the
// explicit -c flag wins, then the kubectl.kubernetes.io/default-container
// annotation, then the pod's first container - matching kubectl's defaulting.
// The pod manifest is fetched through the Argo CD GetResource API.
func (c *Client) ResolvePodContainer(ctx context.Context, resolved *ResolvedApp, podName, requested string) (string, error) {
	if requested = strings.TrimSpace(requested); requested != "" {
		return requested, nil
	}

	manifest, err := c.fetchResourceManifest(ctx, resolved, podName)
	if err != nil {
		return "", err
	}

	type containerSpec struct {
		Name string `json:"name"`
	}
	var pod struct {
		Metadata struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
		Spec struct {
			Containers []containerSpec `json:"containers"`
		} `json:"spec"`
	}
	if err := json.Unmarshal([]byte(manifest), &pod); err != nil {
		return "", fmt.Errorf("failed to parse pod %q manifest from Argo CD: %w", podName, err)
	}

	if name := strings.TrimSpace(pod.Metadata.Annotations["kubectl.kubernetes.io/default-container"]); name != "" {
		return name, nil
	}
	if len(pod.Spec.Containers) == 0 || pod.Spec.Containers[0].Name == "" {
		return "", fmt.Errorf("pod %q manifest has no containers; specify one with -c/--container", podName)
	}
	return pod.Spec.Containers[0].Name, nil
}

// fetchResourceManifest returns the live pod manifest via
// GET /api/v1/applications/{app}/resource (RBAC-filtered server side).
func (c *Client) fetchResourceManifest(ctx context.Context, resolved *ResolvedApp, podName string) (string, error) {
	endpoint, err := c.buildRESTURL(fmt.Sprintf("/api/v1/applications/%s/resource", url.PathEscape(resolved.AppName)), resolved.AppNamespace)
	if err != nil {
		return "", err
	}

	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	q := parsed.Query()
	q.Set("namespace", resolved.TargetNamespace)
	q.Set("resourceName", podName)
	q.Set("version", "v1")
	q.Set("kind", "Pod")
	q.Set("group", "")
	parsed.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.AuthToken)
	req.Header.Set("Accept", "application/json")

	httpClient := c.newHTTPClient(30 * time.Second)
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to fetch pod %q manifest: %w", podName, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("GET %s returned HTTP %d: %s", parsed.Path, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var body struct {
		Manifest string `json:"manifest"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("failed to decode pod %q manifest response: %w", podName, err)
	}
	if strings.TrimSpace(body.Manifest) == "" {
		return "", fmt.Errorf("Argo CD returned an empty manifest for pod %q", podName)
	}
	return body.Manifest, nil
}
