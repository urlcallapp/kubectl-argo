package argocd

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/argoproj/argo-cd/v2/pkg/apiclient/application"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"kubectl-argo/internal/config"
)

// fakePodLogsServer streams the given log entries, then fails the stream with err.
func strPtr(s string) *string { return &s }

func logEntry(content string) *application.LogEntry {
	return &application.LogEntry{
		Content:      strPtr(content),
		TimeStamp:    &metav1.Time{},
		Last:         new(bool),
		TimeStampStr: strPtr(""),
		PodName:      strPtr("web-1"),
	}
}

type fakePodLogsServer struct {
	application.UnimplementedApplicationServiceServer
	entries []*application.LogEntry
	err     error
}

func (f *fakePodLogsServer) PodLogs(_ *application.ApplicationPodLogsQuery, stream application.ApplicationService_PodLogsServer) error {
	for _, entry := range f.entries {
		if err := stream.Send(entry); err != nil {
			return err
		}
	}
	return f.err
}

func startFakePodLogsServer(t *testing.T, srv application.ApplicationServiceServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	application.RegisterApplicationServiceServer(grpcServer, srv)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)
	return lis.Addr().String()
}

// A gRPC stream that dies after delivering output must not trigger the REST
// fallback: re-streaming from the start would duplicate the lines already shown.
func TestStreamPodLogsNoFallbackAfterPartialOutput(t *testing.T) {
	srv := &fakePodLogsServer{
		entries: []*application.LogEntry{logEntry("line-a"), logEntry("line-b")},
		err:     status.Error(codes.Unavailable, "connection reset by peer"),
	}
	addr := startFakePodLogsServer(t, srv)

	cfg := &config.ArgoConfig{ServerAddr: addr, AuthToken: "test-token", PlainText: true}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("client init: %v", err)
	}

	var out strings.Builder
	opts := PodLogsOptions{PodName: "web-1"}
	err = client.StreamPodLogs(context.Background(), logsTestResolved(), opts, &out)
	if err == nil {
		t.Fatalf("expected the mid-stream failure to surface, got nil error")
	}
	if !strings.Contains(err.Error(), "mid-stream") {
		t.Errorf("error should report a mid-stream failure, got: %v", err)
	}
	if out.String() != "line-a\nline-b\n" {
		t.Errorf("partial gRPC output must be preserved exactly once, got %q", out.String())
	}
}

// A gRPC stream that fails before delivering anything still falls back to REST.
func TestStreamPodLogsFallbackWhenGRPCFailsBeforeOutput(t *testing.T) {
	srv := &fakePodLogsServer{
		err: status.Error(codes.Unavailable, "stream rejected"),
	}
	addr := startFakePodLogsServer(t, srv)

	cfg := &config.ArgoConfig{ServerAddr: addr, AuthToken: "test-token", PlainText: true}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("client init: %v", err)
	}

	var out strings.Builder
	opts := PodLogsOptions{PodName: "web-1"}
	err = client.StreamPodLogs(context.Background(), logsTestResolved(), opts, &out)
	// The gRPC server cannot answer the REST fallback, so the attempt must fail
	// without panicking and without fabricating output.
	if err == nil {
		t.Fatalf("expected an error when both transports fail, got nil")
	}
	if out.String() != "" {
		t.Errorf("no output expected when the stream fails before any line, got %q", out.String())
	}
}
