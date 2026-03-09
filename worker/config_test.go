package worker

import "testing"

func TestValidateArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    *WorkerStartupConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid args with http scheme",
			args: &WorkerStartupConfig{
				FunctionsUri:                  "http://127.0.0.1:5000",
				FunctionsWorkerId:             "worker-123",
				FunctionsRequestId:            "request-456",
				FunctionsGrpcMaxMessageLength: DefaultFunctionsGrpcMaxMsgLen,
			},
			wantErr: false,
		},
		{
			name: "valid args with raw address (no scheme) fails url.Parse",
			args: &WorkerStartupConfig{
				FunctionsUri:                  "127.0.0.1:5000",
				FunctionsWorkerId:             "worker-123",
				FunctionsRequestId:            "request-456",
				FunctionsGrpcMaxMessageLength: DefaultFunctionsGrpcMaxMsgLen,
			},
			wantErr: true,
			errMsg:  `invalid --functions-uri provided (127.0.0.1:5000): parse "127.0.0.1:5000": first path segment in URL cannot contain colon`,
		},
		{
			name: "missing functions uri",
			args: &WorkerStartupConfig{
				FunctionsUri:       "",
				FunctionsWorkerId:  "worker-123",
				FunctionsRequestId: "request-456",
			},
			wantErr: true,
			errMsg:  "missing required argument: --functions-uri",
		},
		{
			name: "missing worker id",
			args: &WorkerStartupConfig{
				FunctionsUri:       "http://127.0.0.1:5000",
				FunctionsWorkerId:  "",
				FunctionsRequestId: "request-456",
			},
			wantErr: true,
			errMsg:  "missing required argument: --functions-worker-id",
		},
		{
			name: "missing request id",
			args: &WorkerStartupConfig{
				FunctionsUri:       "http://127.0.0.1:5000",
				FunctionsWorkerId:  "worker-123",
				FunctionsRequestId: "",
			},
			wantErr: true,
			errMsg:  "missing required argument: --functions-request-id",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateArgs(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.errMsg)
				}
				if err.Error() != tc.errMsg {
					t.Errorf("expected error %q, got %q", tc.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			}
		})
	}
}

func TestValidateArgs_CleansUri(t *testing.T) {
	args := &WorkerStartupConfig{
		FunctionsUri:       "http://127.0.0.1:5000",
		FunctionsWorkerId:  "worker-1",
		FunctionsRequestId: "req-1",
	}
	if err := validateArgs(args); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args.FunctionsUri != "127.0.0.1:5000" {
		t.Errorf("expected URI to be cleaned to %q, got %q", "127.0.0.1:5000", args.FunctionsUri)
	}
}

func TestCleanAddressForGrpc(t *testing.T) {
	tests := []struct {
		name    string
		address string
		want    string
	}{
		{
			name:    "http scheme stripped",
			address: "http://127.0.0.1:5000",
			want:    "127.0.0.1:5000",
		},
		{
			name:    "https scheme stripped",
			address: "https://localhost:443",
			want:    "localhost:443",
		},
		{
			name:    "raw address unchanged",
			address: "127.0.0.1:5000",
			want:    "127.0.0.1:5000",
		},
		{
			name:    "non-http scheme unchanged",
			address: "ftp://example.com",
			want:    "ftp://example.com",
		},
		{
			name:    "empty string unchanged",
			address: "",
			want:    "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := CleanAddressForGrpc(tc.address)
			if got != tc.want {
				t.Errorf("CleanAddressForGrpc(%q) = %q, want %q", tc.address, got, tc.want)
			}
		})
	}
}
