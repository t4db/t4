package cli

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestParseAdvertiseClientURLs(t *testing.T) {
	for _, value := range []string{
		"",
		"http://localhost:3379",
		"https://T4.Example.com:443",
		"http://127.0.0.1:1",
		"http://[::1]:65535",
		"http://[2001:db8::1]:3379",
		"HTTP://NODE-A:3379, https://node-b:443 ,\thttp://node-c:3379\t",
	} {
		t.Run(value, func(t *testing.T) {
			got, err := parseAdvertiseClientURLs(value)
			if err != nil {
				t.Fatal(err)
			}
			var want []string
			if value != "" {
				for _, v := range strings.Split(value, ",") {
					want = append(want, strings.Trim(v, " \t\r\n\v\f"))
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v, want %v", got, want)
			}
		})
	}
	for _, value := range []string{
		" ",
		"localhost:3379",
		"ftp://host:3379",
		"http://0.0.0.0:3379",
		"http://[::]:3379",
		"http://[::ffff:0.0.0.0]:3379",
		"http://*:3379",
		"http://host",
		"http://host:0",
		"http://host:65536",
		"http://host:abc",
		"http://host:-1",
		"http://user:secret@host:3379",
		"http://host:3379/",
		"http://host:3379/path",
		"http://host:3379?",
		"http://host:3379?q=x",
		"http://host:3379#",
		"http://host:3379#fragment",
		",http://host:3379",
		"http://host:3379,",
		"http://host:3379,,http://other:3379",
		"http://host:3379,HTTP://HOST:3379",
		"http://ho\nst:3379",
		"http://host:3379\x7f",
		"http://[localhost]:3379",
		"http://[127.0.0.1]:3379",
		"http://::1:3379",
		"http://bad_host:3379",
		"http://host:3379/%zz",
	} {
		t.Run(value, func(t *testing.T) {
			if _, err := parseAdvertiseClientURLs(value); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestAdvertiseClientURLsEnvironment(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{"environment", nil, "http://environment:3379"},
		{"explicit flag", []string{"--advertise-client-urls=https://explicit:443"}, "https://explicit:443"},
		{"explicit empty flag", []string{"--advertise-client-urls="}, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("T4_ADVERTISE_CLIENT_URLS", "http://environment:3379")
			cmd := runCmd()
			cmd.RunE = func(cmd *cobra.Command, _ []string) error {
				got, err := cmd.Flags().GetString("advertise-client-urls")
				if err != nil {
					t.Fatal(err)
				}
				if got != tt.want {
					t.Fatalf("got %q, want %q", got, tt.want)
				}
				if _, err := parseAdvertiseClientURLs(got); err != nil {
					t.Fatal(err)
				}
				return nil
			}
			if err := execute(t, cmd, tt.args...); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInvalidAdvertiseClientURLsBeforeDatabaseStartup(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "unopened")
	cmd := runCmd()
	err := execute(t, cmd, "--data-dir="+dataDir, "--advertise-client-urls=http://0.0.0.0:3379")
	if err == nil || !strings.Contains(err.Error(), "--advertise-client-urls") {
		t.Fatalf("expected advertised URL error, got %v", err)
	}
	if _, err := os.Stat(dataDir); !os.IsNotExist(err) {
		t.Fatalf("database directory was touched: %v", err)
	}
}
