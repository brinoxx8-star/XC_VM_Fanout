// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

// Regression test for the ring clock with no cadence known (follow-up to
// #133). It uses only API that exists on master, so it can be run there
// unchanged: on master it FAILS (the step is taken in full and the parked
// cursor's block leaves the ring), with the window cap it passes.

import (
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// TestParkedCursorSurvivesAStepWithNoCadence: a run of packets with no
// random-access point cuts a block by max_gop_bytes, which forgets the cadence
// (clockStep == 0) — the state #133 deliberately leaves taking a forward step
// in full. A source resuming ten minutes ahead then aged the whole ring in one
// Update and the cursor parked at the edge was reported behind. With the step
// capped at the ring window the edge block survives and the cursor carries on.
func TestParkedCursorSurvivesAStepWithNoCadence(t *testing.T) {
	s := New(4*PacketSize, 32_000) // a 4-packet max_gop_bytes: filler alone cuts blocks
	s.Update(tsfixture.PAT(0x100))
	s.Update(tsfixture.PMT(0x100, 0x101))
	for sec := int64(0); sec < 10; sec++ {
		s.Update(append(tsfixture.KeyframePCR(0x101, sec*90000, sec*90000), tsfixture.Fill(0x101)...))
	}
	var junk []byte
	for i := 0; i < 40; i++ {
		junk = append(junk, tsfixture.Fill(0x101)...)
	}
	s.Update(junk) // blocks cut by size, no keyframe: the cadence is forgotten
	if s.clockStep != 0 {
		t.Fatalf("setup: expected no cadence after the max_gop_bytes cuts, got clockStep=%d", s.clockStep)
	}
	open := s.gops[len(s.gops)-1]
	edge := Cursor{GOP: open.id, Off: len(open.data)} // parked at the live edge, everything read
	s.Update(append(tsfixture.Fill(0x101), tsfixture.KeyframePCR(0x101, 609*90000, 609*90000)...))
	if _, _, _, behind, _ := s.ReadFrom(edge, 1<<20); behind {
		t.Fatalf("a cursor parked at the live edge was dropped as behind by a forward step with no cadence known (%d blocks left)", len(s.gops))
	}
}
