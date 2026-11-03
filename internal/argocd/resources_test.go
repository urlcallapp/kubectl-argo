package argocd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/argoproj/argo-cd/v2/pkg/apis/application/v1alpha1"

	"kubectl-argo/internal/config"
)

func resourcesTestClient(serverAddr string) *Client {
	return &Client{cfg: &config.ArgoConfig{ServerAddr: serverAddr, AuthToken: "test-token", PlainText: true}}
}

func resourcesTestResolved() *ResolvedApp {
	return &ResolvedApp{AppName: "prod-payments", AppNamespace: "argocd", TargetNamespace: "payments-prod"}
}

func TestFilterResourceNodes(t *testing.T) {
	tree := &v1alpha1.ApplicationTree{Nodes: []v1alpha1.ResourceNode{
		{ResourceRef: v1alpha1.ResourceRef{Kind: "Pod", Name: "web-1", Namespace: "payments-prod"}},
		{ResourceRef: v1alpha1.ResourceRef{Kind: "Pod", Name: "web-2", Namespace: "payments-dev"}},
		{ResourceRef: v1alpha1.ResourceRef{Kind: "Service", Name: "web", Namespace: "payments-prod"}},
		{ResourceRef: v1alpha1.ResourceRef{Kind: "pod", Name: "job-1", Namespace: "payments-prod"}},
	}}

	pods := FilterResourceNodes(tree, "Pod", "payments-prod", "")
	if len(pods) != 2 { // web-1 and lower-case "pod" kind both match
		t.Errorf("expected 2 pods in payments-prod, got %d", len(pods))
	}

	services := FilterResourceNodes(tree, "service", "payments-prod", "") // kind match is case-insensitive
	if len(services) != 1 || services[0].Name != "web" {
		t.Errorf("expected the payments-prod service, got %+v", services)
	}

	named := FilterResourceNodes(tree, "Pod", "", "web-2")
	if len(named) != 1 || named[0].Namespace != "payments-dev" {
		t.Errorf("name filter should find web-2 across namespaces, got %+v", named)
	}

	if got := FilterResourceNodes(tree, "Pod", "payments-prod", "missing"); len(got) != 0 {
		t.Errorf("expected no match for missing pod, got %+v", got)
	}
	if got := FilterResourceNodes(nil, "Pod", "", ""); got != nil {
		t.Errorf("nil tree should yield no nodes, got %+v", got)
	}
}

func TestFetchResourceTreeSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/api/v1/applications/prod-payments/resource-tree") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("missing bearer token")
		}
		if r.URL.Query().Get("appNamespace") != "argocd" {
			t.Errorf("missing appNamespace query: %s", r.URL.RawQuery)
		}
		tree := v1alpha1.ApplicationTree{Nodes: []v1alpha1.ResourceNode{
			{ResourceRef: v1alpha1.ResourceRef{Kind: "Pod", Name: "web-1", Namespace: "payments-prod"}},
		}}
		_ = json.NewEncoder(w).Encode(tree)
	}))
	defer server.Close()

	tree, err := resourcesTestClient(server.URL).FetchResourceTree(context.Background(), resourcesTestResolved())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tree.Nodes) != 1 || tree.Nodes[0].Name != "web-1" {
		t.Errorf("unexpected tree: %+v", tree.Nodes)
	}
}

func TestFetchResourceTreeHTTPErrorSurfacesBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"permission denied"}`, http.StatusForbidden)
	}))
	defer server.Close()

	client, clientErr := NewClient(&config.ArgoConfig{ServerAddr: server.URL, AuthToken: "test-token", PlainText: true})
	if clientErr != nil {
		t.Fatalf("client init: %v", clientErr)
	}
	_, err := client.FetchResourceTree(context.Background(), resourcesTestResolved())
	if err == nil {
		t.Fatalf("expected an error for HTTP 403")
	}
	// The gRPC fallback also fails against a plain HTTP server; the message must
	// still carry the REST status and server body.
	for _, want := range []string{"403", "permission denied"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q: %v", want, err)
		}
	}
}
