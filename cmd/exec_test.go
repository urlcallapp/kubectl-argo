package cmd

import "testing"

func TestParseExecArgs(t *testing.T) {
	cases := []struct {
		name        string
		args        []string
		dash        int
		wantPod     string
		wantCommand []string
		wantErr     bool
	}{
		{name: "interactive shell", args: []string{"web-1"}, dash: -1, wantPod: "web-1"},
		{name: "command after dash", args: []string{"web-1", "ls", "-l"}, dash: 1, wantPod: "web-1", wantCommand: []string{"ls", "-l"}},
		{name: "pod form with command", args: []string{"pod/web-1", "date"}, dash: 1, wantPod: "pod/web-1", wantCommand: []string{"date"}},
		{name: "dash but no command", args: []string{"web-1"}, dash: 1, wantErr: true},
		{name: "two pods no dash", args: []string{"web-1", "web-2"}, dash: -1, wantErr: true},
		{name: "two pods before dash", args: []string{"web-1", "web-2", "ls"}, dash: 2, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod, command, err := parseExecArgs(tc.args, tc.dash)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got pod=%q command=%v", pod, command)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if pod != tc.wantPod {
				t.Errorf("pod: got %q, want %q", pod, tc.wantPod)
			}
			if len(command) != len(tc.wantCommand) {
				t.Errorf("command: got %v, want %v", command, tc.wantCommand)
			} else {
				for i := range command {
					if command[i] != tc.wantCommand[i] {
						t.Errorf("command[%d]: got %q, want %q", i, command[i], tc.wantCommand[i])
					}
				}
			}
		})
	}
}

func TestExitErrorCarriesRemoteCode(t *testing.T) {
	var err error = &exitError{code: 42}
	coder, ok := err.(interface{ ExitCode() int })
	if !ok {
		t.Fatalf("exitError does not expose ExitCode")
	}
	if coder.ExitCode() != 42 {
		t.Errorf("got exit code %d, want 42", coder.ExitCode())
	}
}
