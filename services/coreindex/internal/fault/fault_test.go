package fault

import "testing"

func TestParse(t *testing.T) {
	s, err := Parse("slice_rotated:pause:10001")
	if err != nil || s.Point != "slice_rotated" || !s.Pause || s.MinBlock != 10001 {
		t.Fatalf("got %+v, %v", s, err)
	}
	if s, err := Parse(""); s != nil || err != nil {
		t.Fatalf("empty: %+v, %v", s, err)
	}
	for _, bad := range []string{"core_synced", "nowhere:kill", "core_synced:stop", "core_synced:kill:x", "a:b:c:d"} {
		if _, err := Parse(bad); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}

func TestPointIgnoresOtherPointsAndEarlierBlocks(t *testing.T) {
	active = &Spec{Point: "core_synced", MinBlock: 100}
	defer func() { active = nil; observer = nil }()
	var seen []string
	SetObserver(func(p string, _ uint32) { seen = append(seen, p) })
	Point("abi_synced", 500)
	Point("core_synced", 99)
	if len(seen) != 2 {
		t.Fatalf("observer saw %v", seen)
	}
}
