package lb

import (
	"slices"
	"testing"
)

func TestParseUpstreams(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []string // u.String() of each result, in order
		wantErr bool
	}{
		// valid input
		{name: "empty means no upstreams", in: "", want: nil},
		{name: "single", in: "http://127.0.0.1:8081", want: []string{"http://127.0.0.1:8081"}},
		{name: "https", in: "https://example.com", want: []string{"https://example.com"}},
		{name: "trailing slash", in: "http://a:8081/", want: []string{"http://a:8081/"}},
		{name: "ipv6", in: "http://[::1]:8081", want: []string{"http://[::1]:8081"}},
		{
			name: "two ports on one host",
			in:   "http://127.0.0.1:8081,http://127.0.0.1:8082",
			want: []string{"http://127.0.0.1:8081", "http://127.0.0.1:8082"},
		},
		{
			name: "spaces around elements",
			in:   " http://a:8081 , http://b:8081 ",
			want: []string{"http://a:8081", "http://b:8081"},
		},
		{
			name: "order is preserved",
			in:   "http://c:1,http://a:1,http://b:1",
			want: []string{"http://c:1", "http://a:1", "http://b:1"},
		},
		{
			name: "same host different scheme",
			in:   "http://a:8081,https://a:8081",
			want: []string{"http://a:8081", "https://a:8081"},
		},

		// empty elements
		{name: "trailing comma", in: "http://a:8081,", wantErr: true},
		{name: "leading comma", in: ",http://a:8081", wantErr: true},
		{name: "only comma", in: ",", wantErr: true},
		{name: "only spaces", in: "   ", wantErr: true},
		{name: "empty in the middle", in: "http://a:1,,http://b:1", wantErr: true},

		// duplicates
		{name: "duplicate", in: "http://a:8081,http://a:8081", wantErr: true},
		{name: "duplicate different case", in: "http://a:8081,HTTP://A:8081", wantErr: true},
		{name: "duplicate trailing slash", in: "http://a:8081,http://a:8081/", wantErr: true},
		{name: "duplicate with spaces", in: "http://a:8081, http://a:8081", wantErr: true},

		// scheme and host
		{name: "no scheme, host:port", in: "127.0.0.1:8081", wantErr: true},
		{name: "localhost:port parses as scheme", in: "localhost:8081", wantErr: true},
		{name: "wrong scheme", in: "ftp://a:8081", wantErr: true},
		{name: "no host", in: "http://", wantErr: true},
		{name: "port without host", in: "http://:8081", wantErr: true},
		{name: "opaque", in: "http:a", wantErr: true},
		{name: "bad url", in: "http://a b:8081", wantErr: true},

		// parts that must not be there
		{name: "path", in: "http://a:8081/v1", wantErr: true},
		{name: "query", in: "http://a:8081?x=1", wantErr: true},
		{name: "fragment", in: "http://a:8081#x", wantErr: true},
		{name: "userinfo", in: "http://user:pass@a:8081", wantErr: true},

		// one bad element poisons the whole list
		{name: "one bad among good", in: "http://a:1,ftp://b:1,http://c:1", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseUpstreams(tt.in)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseUpstreams(%q) = %v, want error", tt.in, got)
				}
				if got != nil {
					t.Errorf("ParseUpstreams(%q) returned %v together with an error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseUpstreams(%q) unexpected error: %v", tt.in, err)
			}

			gotStrs := make([]string, 0, len(got))
			for _, u := range got {
				if u == nil {
					t.Fatalf("ParseUpstreams(%q) returned a nil *url.URL", tt.in)
				}
				gotStrs = append(gotStrs, u.String())
			}
			if !slices.Equal(gotStrs, tt.want) {
				t.Errorf("ParseUpstreams(%q) = %v, want %v", tt.in, gotStrs, tt.want)
			}
		})
	}
}
