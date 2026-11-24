package cli

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// parseAdvertiseClientURLs validates static discovery endpoints without doing
// DNS lookups or connecting to services that may still be starting.
func parseAdvertiseClientURLs(value string) ([]string, error) {
	if value == "" {
		return nil, nil
	}
	entries := strings.Split(value, ",")
	seen := make(map[string]bool, len(entries))
	for i, entry := range entries {
		entry = strings.Trim(entry, " \t\r\n\v\f")
		invalid := func(reason string) ([]string, error) {
			return nil, fmt.Errorf("entry %d: %s; use an absolute http(s) URL such as https://t4.example.com:443", i+1, reason)
		}
		if entry == "" {
			return invalid("empty URL (remove extra commas)")
		}
		if strings.IndexFunc(entry, func(r rune) bool { return r <= 0x20 || r == 0x7f }) >= 0 {
			return invalid("URL contains whitespace or control characters")
		}
		u, err := url.Parse(entry)
		if err != nil {
			// Do not echo potentially credential-bearing input in startup errors.
			return invalid("malformed URL")
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return invalid("scheme must be http or https")
		}
		if u.User != nil || u.Path != "" || u.RawPath != "" || strings.ContainsAny(entry, "?#") || u.Opaque != "" {
			return invalid("credentials, paths, queries, and fragments are not allowed")
		}
		host, port, err := net.SplitHostPort(u.Host)
		if err != nil || host == "" || port == "" {
			return invalid("a hostname and explicit TCP port are required (bracket IPv6 addresses)")
		}
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 || strings.Trim(port, "0123456789") != "" {
			return invalid("TCP port must be a number in 1..65535")
		}
		if ip := net.ParseIP(host); ip != nil {
			if strings.HasPrefix(u.Host, "[") && !strings.Contains(host, ":") {
				return invalid("brackets are only valid around IPv6 literals")
			}
			if ip.IsUnspecified() {
				return invalid("unspecified/wildcard addresses cannot be advertised")
			}
		} else {
			if strings.ContainsAny(u.Host, "[]") || !validDiscoveryHostname(host) {
				return invalid("invalid DNS hostname or IP address")
			}
		}
		key := strings.ToLower(u.Scheme) + "://" + net.JoinHostPort(strings.ToLower(host), port)
		if seen[key] {
			return invalid("duplicate URL (scheme and DNS hostname are case-insensitive)")
		}
		seen[key] = true
		entries[i] = entry
	}
	return entries, nil
}

func validDiscoveryHostname(host string) bool {
	host = strings.TrimSuffix(host, ".")
	if len(host) == 0 || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' {
				return false
			}
		}
	}
	return true
}
