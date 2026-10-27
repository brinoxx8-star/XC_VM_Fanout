// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/defaults"
)

// TestProBoxKeysBackfilledAndClamped: a config.json written by a panel that
// does not know the new keys (every node on day one) is completed with their
// defaults without touching the rest; explicit values are honoured and clamped.
func TestProBoxKeysBackfilledAndClamped(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"prebuffer_max_sec": 32, "viewer_idle_timeout_sec": 90}`), 0o644); err != nil {
		t.Fatal(err)
	}
	v, wrote, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !wrote {
		t.Fatalf("the missing keys were not backfilled into the file")
	}
	if v.PrebufferMaxSec != 32 || v.ViewerIdleTimeoutSec != 90 {
		t.Fatalf("panel values altered: %+v", v)
	}
	if v.RingGapDiscontinuitySec != defaults.CfgRingGapDiscontinuitySec || v.ViewerResync != defaults.CfgViewerResync || v.ViewerResyncMaxPerMin != defaults.CfgViewerResyncMaxPerMin {
		t.Fatalf("wrong defaults: gap=%d resync=%v max=%d", v.RingGapDiscontinuitySec, v.ViewerResync, v.ViewerResyncMaxPerMin)
	}
	b, _ := os.ReadFile(path)
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"ring_gap_discontinuity_sec", "viewer_resync", "viewer_resync_max_per_min"} {
		if _, ok := raw[k]; !ok {
			t.Fatalf("key %s was not written to the file", k)
		}
	}

	// Explicit values: honoured (0 switches each piece off) and clamped.
	if err := os.WriteFile(path, []byte(`{"ring_gap_discontinuity_sec": 0, "viewer_resync": false, "viewer_resync_max_per_min": 99999}`), 0o644); err != nil {
		t.Fatal(err)
	}
	v, _, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if v.RingGapDiscontinuitySec != 0 || v.ViewerResync || v.ViewerResyncMaxPerMin != 1000 {
		t.Fatalf("explicit values misapplied: gap=%d resync=%v max=%d", v.RingGapDiscontinuitySec, v.ViewerResync, v.ViewerResyncMaxPerMin)
	}
}
