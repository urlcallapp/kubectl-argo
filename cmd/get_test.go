package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/argoproj/argo-cd/v2/pkg/apis/application/v1alpha1"
)

func TestResolveCanonicalKind(t *testing.T) {
	for input, want := range map[string]string{
		"po": "Pod", "pod": "Pod", "pods": "Pod", "PODS": "Pod",
		"svc": "Service", "service": "Service", "services": "Service",
		"ing": "Ingress", "ingress": "Ingress", "ingresses": "Ingress",
	} {
		got, err := resolveCanonicalKind(input)
		if err != nil || got != want {
			t.Errorf("resolveCanonicalKind(%q) = %q, %v; want %q", input, got, err, want)
		}
	}
	if _, err := resolveCanonicalKind("deploy"); err == nil || !strings.Contains(err.Error(), "unsupported resource type") {
		t.Errorf("expected unsupported-type error for deploy, got %v", err)
	}
}

func TestPrintResourceTablePod(t *testing.T) {
	nodes := []v1alpha1.ResourceNode{{
		ResourceRef: v1alpha1.ResourceRef{Kind: "Pod", Name: "web-6d4f8b7c9-x2k9p", Namespace: "payments-prod"},
		Info: []v1alpha1.InfoItem{
			{Name: "Containers", Value: "1/1"},
			{Name: "Status Reason", Value: "Running"},
			{Name: "Restart Count", Value: "2"},
		},
	}}

	var out bytes.Buffer
	if err := printResourceTable("Pod", nodes, false, false, &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rendered := out.String()
	for _, want := range []string{"NAME", "READY", "STATUS", "RESTARTS", "AGE", "web-6d4f8b7c9-x2k9p", "1/1", "Running"} {
		if !strings.Contains(rendered, want) {
			t.Errorf("table missing %q:\n%s", want, rendered)
		}
	}
	if strings.Contains(rendered, "NODE") || strings.Contains(rendered, "HEALTH") {
		t.Errorf("wide-only columns must stay hidden without -o wide:\n%s", rendered)
	}

	out.Reset()
	if err := printResourceTable("Pod", nodes, true, false, &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out.String(), "HEALTH") {
		t.Errorf("-o wide should add NODE/HEALTH columns:\n%s", out.String())
	}

	out.Reset()
	if err := printResourceTable("Pod", nodes, false, true, &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(out.String(), "NAME") {
		t.Errorf("--no-headers should drop the header row:\n%s", out.String())
	}
}

func TestParsePodTarget(t *testing.T) {
	for input, want := range map[string]string{
		"web-1": "web-1", "pod/web-1": "web-1", "pods/web-1": "web-1", "po/web-1": "web-1",
	} {
		got, err := parsePodTarget(input)
		if err != nil || got != want {
			t.Errorf("parsePodTarget(%q) = %q, %v; want %q", input, got, err, want)
		}
	}
	for _, bad := range []string{"deploy/web-1", "svc/web-1", ""} {
		if _, err := parsePodTarget(bad); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
}
