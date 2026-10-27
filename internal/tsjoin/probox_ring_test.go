// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

// Ring-clock (ringClock) tests for the ring-resync change:
//
//   - a forward PCR step larger than the ring window is a jump, not elapsed
//     time (before: up to 60 s was taken in full and pruned the follower parked
//     at the edge wholesale);
//   - with no cadence known (a block cut by max_gop_bytes) the step is capped
//     to the window (before: taken in full, however large);
//   - a forward step after producer silence is an outage (one cadence);
//   - a keyframe-less source keeps pruning (no return of the memory regression
//     that taking the full step without a cadence fixed).

import (
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

const (
	tPMT   = 0x100
	tVideo = 0x101
	hz     = 90000
)

// gopAt is a synthetic GOP opening at second sec of the stream clock: a
// keyframe with PCR and PTS at sec·90000 plus pad filler packets.
func gopAt(sec int64, pad int) []byte {
	b := tsfixture.KeyframePCR(tVideo, sec*hz, sec*hz)
	for i := 0; i < pad; i++ {
		b = append(b, tsfixture.Fill(tVideo)...)
	}
	return b
}

// pcrOnly is a video-PID packet carrying a PCR, no keyframe and no payload:
// what a source without random_access_indicator delivers between blocks.
func pcrOnly(sec int64) []byte {
	p := tsfixture.KeyframePCR(tVideo, sec*hz, sec*hz)
	p[5] = 0x10 // PCR_flag only: no random_access_indicator
	p[1] &^= 0x40
	p[3] = 0x20 // adaptation field, no payload
	return p[:PacketSize]
}

// nullPkt is a null packet (PID 0x1FFF): the keepalive a relay may send
// during a gap, which does not count as signal.
func nullPkt() []byte {
	p := make([]byte, PacketSize)
	p[0], p[1], p[2], p[3] = 0x47, 0x1f, 0xff, 0x10
	for i := 4; i < PacketSize; i++ {
		p[i] = 0xff
	}
	return p
}

// newRing returns a State with a winSec window, PAT/PMT and secs seconds of
// one-second GOPs already published, plus the cursor of a follower parked at
// the edge (it has read everything there is).
func newRing(t *testing.T, winSec int64, secs int64, pad int) (*State, Cursor) {
	t.Helper()
	s := New(1<<20, winSec*1000)
	s.Update(tsfixture.PAT(tPMT))
	s.Update(tsfixture.PMT(tPMT, tVideo))
	for sec := int64(0); sec < secs; sec++ {
		s.Update(gopAt(sec, pad))
	}
	_, c := s.JoinStart(nil, 0)
	for {
		parts, next, atEnd, behind, pin := s.ReadFrom(c, 1<<20)
		s.Unpin(pin)
		if behind {
			t.Fatalf("the follower was reported behind while catching up")
		}
		c = next
		if atEnd && len(parts) == 0 {
			break
		}
		if atEnd {
			break
		}
	}
	return s, c
}

// follow reads from c to the edge and reports whether it fell behind and how many bytes it took.
func follow(s *State, c Cursor) (next Cursor, behind bool, n int) {
	for {
		parts, nx, atEnd, b, pin := s.ReadFrom(c, 1<<20)
		s.Unpin(pin)
		if b {
			return c, true, n
		}
		for _, p := range parts {
			n += len(p)
		}
		c = nx
		if atEnd {
			return c, false, n
		}
	}
}

// resume is the signal coming back as the ring sees it: bytes before the
// keyframe (PAT/PMT, tail of the previous GOP) and then the keyframe opening
// the new block with the clock at sec, in the SAME Update.
func resume(sec int64, pad int) []byte {
	b := tsfixture.PAT(tPMT)
	b = append(b, tsfixture.PMT(tPMT, tVideo)...)
	for i := 0; i < 8; i++ {
		b = append(b, tsfixture.Fill(tVideo)...)
	}
	return append(b, gopAt(sec, pad)...)
}

// TestStepPastWindowKeepsParkedFollower (P1): with P=32 s and a full ring, a
// +45 s PCR jump (≤ 60 s, "real time" for the previous ringClock) pruned the
// edge block in the same Update and the parked follower left as behind. With
// the window cap the block survives and the follower carries on.
func TestStepPastWindowKeepsParkedFollower(t *testing.T) {
	s, c := newRing(t, 32, 40, 20)
	edge := s.gops[len(s.gops)-1].id
	s.Update(resume(39+45, 20)) // the source clock kept running for 45 s
	if s.gops[0].id > edge {
		t.Fatalf("the edge block (id %d) left the ring: first=%d", edge, s.gops[0].id)
	}
	c, behind, n := follow(s, c)
	if behind {
		t.Fatalf("parked follower dropped as behind after a 45 s jump")
	}
	if n == 0 {
		t.Fatalf("the follower did not receive the resumed stream")
	}
	// And the stream goes on normally afterwards: 10 s more and the ring prunes the old blocks.
	for sec := int64(85); sec < 95; sec++ {
		s.Update(gopAt(sec, 20))
	}
	if _, behind, _ = follow(s, c); behind {
		t.Fatalf("behind while reading on after the resume")
	}
	if _, span, _ := s.RingStats(); span > 33000 {
		t.Fatalf("the ring did not go back to pruning by duration: span=%d ms", span)
	}
}

// TestStepPastWindowLegacyWouldDrop documents the "before": the same sequence
// measured in stream time exceeds the window, which is what the previous code
// pruned on. It witnesses that the test above exercises the case.
func TestStepPastWindowLegacyWouldDrop(t *testing.T) {
	s, _ := newRing(t, 32, 40, 20)
	tEdge := s.gops[len(s.gops)-1].t
	step := int64(45) * hz
	if step <= maxCadence && step > s.ring90 {
		// Before: clockT += step → newest − tEdge = 45 s > 32 s → prune released the edge.
		if newest := tEdge + step; newest-tEdge <= s.ring90 {
			t.Fatalf("witness broken")
		}
	} else {
		t.Fatalf("the scenario is not in (window, maxCadence]: step=%d ring90=%d", step, s.ring90)
	}
}

// TestFullStepWithoutCadenceIsCappedToWindow (P2): after a block cut by
// max_gop_bytes with no keyframe (clockStep = 0) the previous code took the
// next step IN FULL however large — minutes or hours — and emptied the ring at
// once. Now it advances exactly the window: the edge block survives.
func TestFullStepWithoutCadenceIsCappedToWindow(t *testing.T) {
	s := New(4*PacketSize, 32*1000) // a 4-packet max_gop_bytes: filler cuts blocks without a keyframe
	s.Update(tsfixture.PAT(tPMT))
	s.Update(tsfixture.PMT(tPMT, tVideo))
	for sec := int64(0); sec < 10; sec++ {
		s.Update(gopAt(sec, 2))
	}
	// A run of filler with no keyframe: blocks are cut by size → clockStep = 0.
	var junk []byte
	for i := 0; i < 40; i++ {
		junk = append(junk, tsfixture.Fill(tVideo)...)
	}
	s.Update(junk)
	if s.clockStep != 0 {
		t.Fatalf("the scenario did not leave clockStep = 0 (%d)", s.clockStep)
	}
	_, c := s.JoinStart(nil, 0)
	c, _, _ = follow(s, c)
	edge := s.gops[len(s.gops)-1].id
	before := s.clockT
	s.Update(resume(9+600, 2)) // a ten-minute jump, no cadence known
	if got := s.clockT - before; got != s.ring90 {
		t.Fatalf("clock advance without a cadence = %d ticks, want exactly the window %d", got, s.ring90)
	}
	if s.gops[0].id > edge {
		t.Fatalf("the edge block (id %d) left the ring: first=%d", edge, s.gops[0].id)
	}
	if _, behind, _ := follow(s, c); behind {
		t.Fatalf("parked follower dropped after the step without a cadence")
	}
}

// TestSilenceMakesForwardStepADiscontinuity: a forward step arriving after 10 s
// of wall-clock time without signal is an outage: the clock advances one
// cadence and the older GOPs stay in the ring. Since #133 a step beyond
// 2×cadence + hls_target is already a jump; here the step (+2 s at a 1 s
// cadence, no HLS view) stays INSIDE that threshold and only the silence turns
// it into an outage. Null keepalives do not count as signal.
func TestSilenceMakesForwardStepADiscontinuity(t *testing.T) {
	s, c := newRing(t, 32, 40, 4)
	wall := time.Unix(1_700_000_000, 0)
	s.SetClock(func() time.Time { return wall })
	s.SetGapDiscontinuity(5 * time.Second)
	s.Update(gopAt(40, 4)) // sets lastDataAt on the injected clock
	c, _, _ = follow(s, c)
	nGops, first := len(s.gops), s.gops[0].id
	before := s.clockT

	wall = wall.Add(3 * time.Second)
	s.Update(nullPkt()) // relay keepalive: not signal
	wall = wall.Add(7 * time.Second)
	s.Update(resume(40+2, 4)) // 10 s of silence and the source clock 2 s ahead (inside the #133 threshold)

	if got, want := s.clockT-before, int64(1*hz); got != want {
		t.Fatalf("clock advance after the silence = %d ticks, want one cadence (%d)", got, want)
	}
	// One cadence of ageing: at most the oldest block leaves, not 26.
	if len(s.gops) < nGops || s.gops[0].id > first+1 {
		t.Fatalf("the resume pruned the ring wholesale: %d → %d blocks, first %d → %d", nGops, len(s.gops), first, s.gops[0].id)
	}
	if _, behind, _ := follow(s, c); behind {
		t.Fatalf("parked follower dropped")
	}
	// The silence is consumed by that keyframe: the next normal step is real
	// time again.
	before = s.clockT
	s.Update(gopAt(43, 4))
	if got := s.clockT - before; got != 1*hz {
		t.Fatalf("after consuming the silence the normal step advanced %d ticks", got)
	}
}

// TestSilenceRuleOffIsUpstream: with ring_gap_discontinuity_sec = 0 the clock
// is upstream's: a +2 s step (inside the #133 threshold, 2×cadence + hls_target)
// is taken in full even after a silence, and a +26 s step is a one-cadence jump
// by the #133 rule itself (not by ours).
func TestSilenceRuleOffIsUpstream(t *testing.T) {
	s, _ := newRing(t, 32, 40, 4)
	wall := time.Unix(1_700_000_000, 0)
	s.SetClock(func() time.Time { return wall })
	s.SetGapDiscontinuity(0)
	before := s.clockT
	wall = wall.Add(10 * time.Second)
	s.Update(resume(39+2, 4))
	if got := s.clockT - before; got != 2*hz {
		t.Fatalf("with the rule off the 2 s step after the silence advanced %d ticks, want the full 2 s", got)
	}
	before = s.clockT
	s.Update(resume(41+26, 4))
	if got := s.clockT - before; got != 2*hz { // the last cadence learned was the 2 s step
		t.Fatalf("a 26 s step must be one cadence (2 s) by the #133 rule: advanced %d ticks", got)
	}
}

// TestKeyframelessSourceStillPrunes (regression): a keyframe-less source
// (radio) cuts blocks by max_gop_bytes that span minutes of PCR. The previous
// code took those steps in full so the ring would age; with the window cap it
// advances one window per block, which still prunes: the ring never retains
// more than two blocks.
func TestKeyframelessSourceStillPrunes(t *testing.T) {
	s := New(8*PacketSize, 32*1000)
	s.Update(tsfixture.PAT(tPMT))
	s.Update(tsfixture.PMT(tPMT, tVideo))
	for blk := int64(0); blk < 50; blk++ {
		// Each block: a PCR 100 s after the previous one and filler up to the cut.
		b := pcrOnly(blk * 100)
		for i := 0; i < 9; i++ {
			b = append(b, tsfixture.Fill(tVideo)...)
		}
		s.Update(b)
		if len(s.gops) > 3 {
			t.Fatalf("the keyframe-less source retains %d blocks at block %d: the clock has frozen", len(s.gops), blk)
		}
	}
}

// TestBackwardStepStaysOneCadence: a producer restart with a new PCR base
// behind the old one is still one cadence (unchanged): the parked follower
// carries on.
func TestBackwardStepStaysOneCadence(t *testing.T) {
	s, c := newRing(t, 32, 40, 4)
	before := s.clockT
	s.Update(resume(1, 4)) // the new instance starts near zero
	if got := s.clockT - before; got != 1*hz {
		t.Fatalf("backward step advanced %d ticks", got)
	}
	if _, behind, _ := follow(s, c); behind {
		t.Fatalf("parked follower dropped on a backward restart")
	}
}
