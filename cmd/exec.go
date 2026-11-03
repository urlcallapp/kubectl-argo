package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"kubectl-argo/internal/argocd"
)

var (
	flagExecContainer string
	flagExecStdin     bool
	flagExecTTY       bool
	flagExecShell     string
)

var execCmd = &cobra.Command{
	Use:   "exec (POD | pod/POD) [-c CONTAINER] [-i] [-t] [-- COMMAND [ARGS...]]",
	Short: "Execute a command or open a shell in a pod via the Argo CD terminal API",
	Long: `Runs a command in a pod container through Argo CD's terminal WebSocket API,
without requiring a kubeconfig or direct Kubernetes cluster access. The pod's
Kubernetes namespace is resolved to its Argo CD Application via -n/--namespace
(exactly like get and logs).

With a COMMAND after "--", the command runs through the container's login shell
and kubectl-argo exits with the remote exit code. Without a COMMAND, an
interactive shell session opens (requires a terminal). Argo CD runs the session
through its exec.shells allow-list (bash or sh by default), and the Argo CD
admin must have the exec feature enabled.`,
	Example: `  # Open an interactive shell in pod web-6d4f8b (namespace payments-prod)
  kubectl-argo exec -it web-6d4f8b -n payments-prod

  # Run a single command and exit with its status code
  kubectl-argo exec web-6d4f8b -n payments-prod -- ls -l /etc

  # Exec into a specific container of a multi-container pod
  kubectl-argo exec -it web-6d4f8b -c istio-proxy -n payments-prod

  # Pipe stdin into a remote command
  echo "show databases;" | kubectl-argo exec -i db-0 -n payments-prod -- mysql -u root`,
	Args: cobra.MinimumNArgs(1),
	RunE: runExec,
}

func init() {
	execCmd.Flags().StringVarP(&flagExecContainer, "container", "c", "", "Container name (defaults to the pod's kubectl.kubernetes.io/default-container annotation, then its first container)")
	execCmd.Flags().BoolVarP(&flagExecStdin, "stdin", "i", false, "Pass stdin to the container")
	execCmd.Flags().BoolVarP(&flagExecTTY, "tty", "t", false, "Attach to the session as an interactive terminal")
	execCmd.Flags().StringVar(&flagExecShell, "shell", "", "Request a specific remote shell (must be in the server's exec.shells allow-list)")

	rootCmd.AddCommand(execCmd)
}

// exitError carries a remote command's exit code so Execute can propagate it.
type exitError struct{ code int }

func (e *exitError) Error() string {
	return fmt.Sprintf("command terminated with exit code %d", e.code)
}
func (e *exitError) ExitCode() int { return e.code }

// parseExecArgs splits the pod target from the command following "--".
func parseExecArgs(args []string, dash int) (podTarget string, command []string, err error) {
	switch {
	case dash < 0:
		if len(args) != 1 {
			return "", nil, fmt.Errorf("exec accepts exactly one pod; place the command after --, e.g. kubectl argo exec POD -n NS -- ls -l")
		}
		return args[0], nil, nil
	case dash != 1:
		return "", nil, fmt.Errorf("exec accepts exactly one pod before --, got %d", dash)
	case len(args) == dash:
		return "", nil, fmt.Errorf("no command specified after --")
	default:
		return args[0], args[dash:], nil
	}
}

func runExec(cmd *cobra.Command, args []string) error {
	podTarget, command, err := parseExecArgs(args, cmd.ArgsLenAtDash())
	if err != nil {
		return err
	}
	podName, err := parsePodTarget(podTarget)
	if err != nil {
		return err
	}

	client, err := ClientFromContext(cmd.Context())
	if err != nil {
		return err
	}
	resolvedApp, err := ResolvedAppFromContext(cmd.Context())
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	container, err := client.ResolvePodContainer(ctx, resolvedApp, podName, flagExecContainer)
	if err != nil {
		return err
	}

	session, err := client.StartExecSession(ctx, resolvedApp, argocd.ExecOptions{
		PodName:   podName,
		Container: container,
		Shell:     strings.TrimSpace(flagExecShell),
	})
	if err != nil {
		return err
	}
	defer func() { _ = session.Close() }()

	stdinIsTTY := term.IsTerminal(int(os.Stdin.Fd()))
	interactive := flagExecTTY || flagExecStdin || (len(command) == 0 && stdinIsTTY)

	if len(command) == 0 && !interactive {
		return fmt.Errorf("no command specified: pass one after --, or run from a terminal for an interactive shell")
	}

	if len(command) > 0 {
		return runExecCommand(ctx, session, command, stdinIsTTY, interactive)
	}
	return runInteractiveShell(ctx, session, stdinIsTTY)
}

// runExecCommand runs COMMAND through the remote shell and propagates its exit code.
func runExecCommand(ctx context.Context, session *argocd.ExecSession, command []string, stdinIsTTY, interactive bool) error {
	restore, err := makeRawIfTTY(interactive && stdinIsTTY)
	if err != nil {
		return err
	}
	defer restore()

	if interactive && stdinIsTTY {
		sendInitialResize(session)
		go watchResize(ctx, session)
	}
	if flagExecStdin {
		go pumpStdin(ctx, session)
	}

	code, err := session.Run(ctx, argocd.ShellQuote(command), os.Stdout)
	if err != nil {
		return err
	}
	if code != 0 {
		return &exitError{code: code}
	}
	return nil
}

// runInteractiveShell opens a plain interactive shell session: raw terminal,
// bidirectional streaming and window-resize forwarding, no command injection.
func runInteractiveShell(ctx context.Context, session *argocd.ExecSession, stdinIsTTY bool) error {
	restore, err := makeRawIfTTY(stdinIsTTY)
	if err != nil {
		return err
	}
	defer restore()

	if stdinIsTTY {
		sendInitialResize(session)
		go watchResize(ctx, session)
	}
	go pumpStdin(ctx, session)

	return session.Stream(ctx, func(data string) {
		_, _ = io.WriteString(os.Stdout, data)
	})
}

func makeRawIfTTY(enabled bool) (func(), error) {
	noop := func() {}
	if !enabled {
		return noop, nil
	}
	fd := int(os.Stdin.Fd())
	previous, err := term.MakeRaw(fd)
	if err != nil {
		return noop, fmt.Errorf("failed to switch terminal to raw mode: %w", err)
	}
	return func() { _ = term.Restore(fd, previous) }, nil
}

func sendInitialResize(session *argocd.ExecSession) {
	cols, rows, err := term.GetSize(int(os.Stdin.Fd()))
	if err == nil && cols > 0 && rows > 0 {
		_ = session.Resize(uint16(cols), uint16(rows))
	}
}

func watchResize(ctx context.Context, session *argocd.ExecSession) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGWINCH)
	defer signal.Stop(sigCh)
	for {
		select {
		case <-ctx.Done():
			return
		case <-sigCh:
			sendInitialResize(session)
		}
	}
}

func pumpStdin(ctx context.Context, session *argocd.ExecSession) {
	buf := make([]byte, 4096)
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := os.Stdin.Read(buf)
		if n > 0 {
			cols, rows, sizeErr := term.GetSize(int(os.Stdin.Fd()))
			if sizeErr != nil {
				cols, rows = 0, 0
			}
			if sendErr := session.Send(string(buf[:n]), uint16(cols), uint16(rows)); sendErr != nil {
				return
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				_, _ = fmt.Fprintf(os.Stderr, "reading stdin failed: %v\n", err)
			}
			return
		}
	}
}
