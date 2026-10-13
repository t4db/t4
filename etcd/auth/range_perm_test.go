package auth

import "testing"

func TestCoversRange(t *testing.T) {
	pub := Permission{Key: "/pub/", RangeEnd: "/pub0", PermType: READ}
	a := Permission{Key: "/a", RangeEnd: "/c", PermType: READ}
	c := Permission{Key: "/c", RangeEnd: "/e", PermType: READWRITE}
	point := Permission{Key: "/k", PermType: WRITE}
	from := Permission{Key: "/m", RangeEnd: "\x00", PermType: READ}
	for _, tc := range []struct {
		name          string
		perms         []Permission
		key, rangeEnd string
		pt            PermType
		want          bool
	}{
		{"key inside a prefix", []Permission{pub}, "/pub/x", "", READ, true},
		{"key outside", []Permission{pub}, "/secret", "", READ, false},
		{"prefix inside", []Permission{pub}, "/pub/", "/pub0", READ, true},
		{"range starting inside, ending past", []Permission{pub}, "/pub/", "/zzz", READ, false},
		{"from a key inside to the end", []Permission{pub}, "/pub/", "\x00", READ, false},
		{"wrong type", []Permission{pub}, "/pub/x", "", WRITE, false},
		{"two permissions cover a range between them", []Permission{a, c}, "/b", "/d", READ, true},
		{"a gap between permissions", []Permission{a, {Key: "/d", RangeEnd: "/e", PermType: READ}}, "/b", "/d0", READ, false},
		{"read and write permission covers read", []Permission{c}, "/c", "/d", READ, true},
		{"point permission covers its key", []Permission{point}, "/k", "", WRITE, true},
		{"point permission covers the range of its key alone", []Permission{point}, "/k", "/k\x00", WRITE, true},
		{"point permission does not cover a wider range", []Permission{point}, "/k", "/l", WRITE, false},
		{"open-ended permission covers to the end", []Permission{from}, "/n", "\x00", READ, true},
		{"open-ended permission covers a range in it", []Permission{from}, "/n", "/p", READ, true},
		{"open-ended permission does not cover before it", []Permission{from}, "/l", "/n", READ, false},
		{"empty range", []Permission{pub}, "/pub/b", "/pub/a", READ, true},
		{"no permissions", nil, "/x", "", READ, false},
	} {
		if got := coversRange(tc.perms, tc.key, tc.rangeEnd, tc.pt); got != tc.want {
			t.Errorf("%s: coversRange(%q, %q) = %v, want %v", tc.name, tc.key, tc.rangeEnd, got, tc.want)
		}
	}
}
