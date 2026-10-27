// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

import (
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/config"
	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// TestApplyConfigCarriesResyncKeys: the three keys travel through the same
// ApplyConfig the config poll already calls on a changed file, so a live edit
// of config.json retunes them without a restart — including the producer
// silence, which is pushed to every EXISTING stream's ring (checked through
// the ring's own behaviour: a +5 s step inside the cadence threshold after a
// silence longer than the setting ages the ring by one cadence with the rule
// on, and by the full 5 s with it off).
func TestApplyConfigCarriesResyncKeys(t *testing.T) {
	mgr := NewManager(1<<20, 32_000, 6, 6, time.Hour)
	st := mgr.GetOrCreate("7")
	st.Publish(tsfixture.PAT(0x100))
	st.Publish(tsfixture.PMT(0x100, 0x101))
	if !mgr.viewerResync.Load() || mgr.viewerResyncMaxPerMin.Load() != 6 || time.Duration(mgr.ringGapDiscNS.Load()) != 5*time.Second {
		t.Fatalf("defaults: resync=%v max=%d gap=%s", mgr.viewerResync.Load(), mgr.viewerResyncMaxPerMin.Load(), time.Duration(mgr.ringGapDiscNS.Load()))
	}

	v := config.Defaults()
	v.ViewerResync = false
	v.ViewerResyncMaxPerMin = 3
	v.RingGapDiscontinuitySec = 1
	mgr.ApplyConfig(v)
	if mgr.viewerResync.Load() || mgr.viewerResyncMaxPerMin.Load() != 3 || time.Duration(mgr.ringGapDiscNS.Load()) != time.Second {
		t.Fatalf("after ApplyConfig: resync=%v max=%d gap=%s", mgr.viewerResync.Load(), mgr.viewerResyncMaxPerMin.Load(), time.Duration(mgr.ringGapDiscNS.Load()))
	}

	// The existing stream's ring got the 1 s silence rule: ten 1 s GOPs, a
	// 1.3 s pause, then a keyframe 5 s ahead — one cadence of ageing, not five.
	sec := int64(0)
	for ; sec < 10; sec++ {
		st.Publish(tsfixture.KeyframePCR(0x101, sec*90000, sec*90000))
	}
	_, before, _ := st.Hub.RingStats()
	time.Sleep(1300 * time.Millisecond)
	sec += 5
	st.Publish(tsfixture.KeyframePCR(0x101, sec*90000, sec*90000))
	if _, after, _ := st.Hub.RingStats(); after-before != 1000 {
		t.Fatalf("silence rule not applied to the existing ring: span grew %d ms, want 1000", after-before)
	}

	// Rule switched off live: the same step after the same pause is taken in full.
	v.RingGapDiscontinuitySec = 0
	mgr.ApplyConfig(v)
	_, before, _ = st.Hub.RingStats()
	time.Sleep(1300 * time.Millisecond)
	sec += 5
	st.Publish(tsfixture.KeyframePCR(0x101, sec*90000, sec*90000))
	if _, after, _ := st.Hub.RingStats(); after-before != 5000 {
		t.Fatalf("rule off not applied to the existing ring: span grew %d ms, want 5000", after-before)
	}
}
