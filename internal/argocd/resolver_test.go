package argocd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kubectl-argo/internal/config"
)

func pickTestResolver(apps map[string][]ResolvedApp) *Resolver {
	return &Resolver{
		cacheTTL: defaultCacheTTL,
		mappings: apps,
	}
}

var twoApps = map[string][]ResolvedApp{
	"checkout-prod": {
		{AppName: "us-east1-payments", TargetNamespace: "checkout-prod", DestinationName: "us-east1", Project: "prod"},
		{AppName: "eu-west1-payments", TargetNamespace: "checkout-prod", DestinationName: "eu-west1", Project: "prod"},
	},
}

func TestPickSingleMatch(t *testing.T) {
	r := pickTestResolver(map[string][]ResolvedApp{
		"payments-prod": {{AppName: "prod-payments", TargetNamespace: "payments-prod"}},
	})
	app, err := r.pick("payments-prod", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if app == nil || app.AppName != "prod-payments" {
		t.Fatalf("expected prod-payments, got %+v", app)
	}
}

func TestPickAmbiguousNamespaceRequiresExplicitApp(t *testing.T) {
	r := pickTestResolver(twoApps)
	app, err := r.pick("checkout-prod", "")
	if err == nil {
		t.Fatalf("expected ambiguity error, got app %+v", app)
	}
	for _, want := range []string{"us-east1-payments", "eu-west1-payments", "--app", "2 Argo CD applications"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ambiguity error missing %q:\n%s", want, err.Error())
		}
	}
}

func TestPickExplicitAppSelectsTarget(t *testing.T) {
	r := pickTestResolver(twoApps)
	app, err := r.pick("checkout-prod", "eu-west1-payments")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if app == nil || app.AppName != "eu-west1-payments" {
		t.Fatalf("expected eu-west1-payments, got %+v", app)
	}
}

func TestPickExplicitAppMismatchListsCandidates(t *testing.T) {
	r := pickTestResolver(twoApps)
	_, err := r.pick("checkout-prod", "does-not-exist")
	if err == nil {
		t.Fatal("expected error for unknown --app")
	}
	if !strings.Contains(err.Error(), "us-east1-payments") || !strings.Contains(err.Error(), "eu-west1-payments") {
		t.Errorf("error should list candidates, got: %s", err.Error())
	}
}

func TestPickUnknownNamespaceReturnsNilNil(t *testing.T) {
	r := pickTestResolver(twoApps)
	app, err := r.pick("no-such-ns", "")
	if app != nil || err != nil {
		t.Fatalf("expected (nil, nil), got (%+v, %v)", app, err)
	}
}

// Legacy caches (single ResolvedApp per namespace) must fail to parse so the
// resolver falls back to a fresh ApplicationService.List instead of reading
// stale or mis-shapen data.
func TestLoadDiskCacheLegacyFormatFailsGracefully(t *testing.T) {
	legacy := `{"server_addr":"argocd.example.com:443","updated_at":"` + time.Now().UTC().Format(time.RFC3339) + `","mappings":{"payments-prod":{"app_name":"prod-payments","target_namespace":"payments-prod"}}}`
	path := filepath.Join(t.TempDir(), "namespace_cache.json")
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	r := pickTestResolver(nil)
	r.cachePath = path
	if err := r.loadDiskCache(); err == nil {
		t.Fatal("expected legacy cache format to be rejected")
	}
}

func TestLoadDiskCacheCurrentFormat(t *testing.T) {
	current := `{"server_addr":"argocd.example.com:443","updated_at":"` + time.Now().UTC().Format(time.RFC3339) + `","mappings":{"checkout-prod":[{"app_name":"us-east1-payments","target_namespace":"checkout-prod"},{"app_name":"eu-west1-payments","target_namespace":"checkout-prod"}]}}`
	path := filepath.Join(t.TempDir(), "namespace_cache.json")
	if err := os.WriteFile(path, []byte(current), 0o600); err != nil {
		t.Fatal(err)
	}
	r := pickTestResolver(nil)
	r.cachePath = path
	r.client = &Client{cfg: &config.ArgoConfig{ServerAddr: "argocd.example.com:443"}}
	if err := r.loadDiskCache(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := len(r.mappings["checkout-prod"]); got != 2 {
		t.Fatalf("expected 2 cached applications, got %d", got)
	}
	// Cached ambiguity must surface the same way as a fresh listing.
	if _, err := r.pick("checkout-prod", ""); err == nil {
		t.Fatal("expected ambiguity error from cached mappings")
	}
}
