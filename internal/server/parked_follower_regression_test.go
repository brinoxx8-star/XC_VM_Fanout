// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

// Regression tests for the parked-follower eviction (follow-up to #133 /
// #130). They use only API that exists on master, so the same file can be run
// against master unchanged: there the burst tests FAIL (every follower parked
// at the live edge gets EOF with "fell behind the ring"), with the re-seat in
// serveLive they pass. The single-cadence-jump test passes on master too: that
// is the case #133 fixed, and it is kept here so the two can be told apart.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// pfViewer is a live-TS client that keeps reading and counts bytes; done is
// closed when the daemon ends the response.
type pfViewer struct {
	got  atomic.Int64
	done chan struct{}
	err  error
}

func (v *pfViewer) closed() bool {
	select {
	case <-v.done:
		return true
	default:
		return false
	}
}

// pfStart connects n viewers to url; rate > 0 limits each reader to rate
// bytes/s (a player drains at the video's pace, not at link speed).
func pfStart(t *testing.T, client *http.Client, url string, n int, rate int64) []*pfViewer {
	t.Helper()
	vs := make([]*pfViewer, n)
	ready := make(chan struct{}, n)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		for _, v := range vs {
			<-v.done
		}
	})
	for i := range vs {
		v := &pfViewer{done: make(chan struct{})}
		vs[i] = v
		go func(v *pfViewer) {
			defer close(v.done)
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			resp, err := client.Do(req)
			if err != nil {
				v.err = err
				ready <- struct{}{}
				return
			}
			defer resp.Body.Close()
			ready <- struct{}{}
			buf := make([]byte, 32<<10)
			start := time.Now()
			for {
				n, err := resp.Body.Read(buf)
				v.got.Add(int64(n))
				if rate > 0 {
					if ahead := time.Duration(float64(v.got.Load())/float64(rate)*float64(time.Second)) - time.Since(start); ahead > 0 {
						time.Sleep(ahead)
					}
				}
				if err != nil {
					if err != io.EOF {
						v.err = err
					}
					return
				}
			}
		}(v)
	}
	for range vs {
		<-ready
	}
	return vs
}

// pfWaitParked waits until every viewer has drained the ring (counters stop
// moving) and is parked at the live edge.
func pfWaitParked(t *testing.T, st *Stream, vs []*pfViewer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	last := make([]int64, len(vs))
	stable := 0
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		if pfClosed(vs) > 0 {
			// Let the rest of the wave land before reporting, so the count says
			// how many of the parked viewers the prune took.
			time.Sleep(3 * time.Second)
			t.Fatalf("%d of %d viewers were closed (EOF) while draining the ring", pfClosed(vs), len(vs))
		}
		st.mu.Lock()
		refs := st.refs
		st.mu.Unlock()
		moved := refs != len(vs)
		for i, v := range vs {
			if g := v.got.Load(); g != last[i] {
				last[i], moved = g, true
			}
		}
		if moved {
			stable = 0
			continue
		}
		if stable++; stable >= 4 {
			return
		}
	}
	t.Fatalf("the viewers never parked at the live edge")
}

func pfClosed(vs []*pfViewer) int {
	n := 0
	for _, v := range vs {
		if v.closed() {
			n++
		}
	}
	return n
}

func pfSnapshot(vs []*pfViewer) []int64 {
	out := make([]int64, len(vs))
	for i, v := range vs {
		out[i] = v.got.Load()
	}
	return out
}

// pfExpectAllKept fails if any viewer was closed, or did not receive at least
// min bytes more than before within timeout.
func pfExpectAllKept(t *testing.T, vs []*pfViewer, before []int64, min int64, timeout time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok := true
		closed := 0
		for i, v := range vs {
			if v.closed() {
				closed++
			}
			if v.got.Load()-before[i] < min {
				ok = false
			}
		}
		if closed > 0 {
			time.Sleep(3 * time.Second) // let the rest of the wave land so the count is the whole of it
			t.Fatalf("%s: %d of %d parked viewers were closed (EOF) instead of receiving the resumed stream", what, pfClosed(vs), len(vs))
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			for i, v := range vs {
				if v.got.Load()-before[i] < min {
					t.Fatalf("%s: viewer %d received nothing after the resume (+%d bytes)", what, i, v.got.Load()-before[i])
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// pfGOP is a one-second GOP opening at sec: a keyframe with PCR and PTS plus
// pad filler packets.
func pfGOP(sec int64, pad int) []byte {
	b := tsfixture.KeyframePCR(0x101, sec*90000, sec*90000)
	for i := 0; i < pad; i++ {
		b = append(b, tsfixture.Fill(0x101)...)
	}
	return b
}

// pfSetup: a Manager with a ringSec ring, a stream with secs seconds published
// and an HTTP server over the client handler.
func pfSetup(t *testing.T, ringSec int, secs int64, pad int) (*Manager, *Stream, *httptest.Server) {
	t.Helper()
	mgr := NewManager(10<<20, int64(ringSec)*1000, 6, 6, time.Hour)
	mgr.viewerIdleNS.Store(0) // no idle drop: a closed session here means "behind"
	st := mgr.GetOrCreate("5")
	st.Publish(tsfixture.PAT(0x100))
	st.Publish(tsfixture.PMT(0x100, 0x101))
	for sec := int64(0); sec < secs; sec++ {
		st.Publish(pfGOP(sec, pad))
	}
	ts := httptest.NewServer(mgr.ClientHandler())
	t.Cleanup(ts.Close)
	return mgr, st, ts
}

// TestParkedFollowersSurviveASingleCadenceJump: the case #133 fixed. A 45 s
// outage whose source clock kept running resumes with one forward step of
// +45 s on a 32 s ring; the parked followers must keep their connection.
// Passes on master (since #133) and with this change.
func TestParkedFollowersSurviveASingleCadenceJump(t *testing.T) {
	_, st, ts := pfSetup(t, 32, 40, 100)
	vs := pfStart(t, ts.Client(), ts.URL+"/live/5?prebuffer=6&c=v", 30, 0)
	pfWaitParked(t, st, vs)
	before := pfSnapshot(vs)
	head := append(tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101)...)
	st.Publish(append(head, pfGOP(39+45, 100)...))
	for sec := int64(85); sec < 90; sec++ {
		st.Publish(pfGOP(sec, 100))
		time.Sleep(20 * time.Millisecond)
	}
	pfExpectAllKept(t, vs, before, 6*100*188, 3*time.Second, "single +45 s step")
}

// TestParkedFollowersSurviveMinutesOfVideoInOneUpdate: after an outage the
// source does not jump, it catches up — ten minutes of coherent one-second
// GOPs land in a single Publish. Every step is a plausible cadence, so the
// ring clock advances ten minutes in one Update and prune releases the block
// every parked follower sits on before any of them is scheduled. On master
// all 60 are dropped as "fell behind the ring"; with the re-seat none is.
func TestParkedFollowersSurviveMinutesOfVideoInOneUpdate(t *testing.T) {
	_, st, ts := pfSetup(t, 32, 40, 100)
	vs := pfStart(t, ts.Client(), ts.URL+"/live/5?prebuffer=6&c=v", 60, 0)
	pfWaitParked(t, st, vs)
	before := pfSnapshot(vs)
	var burst []byte
	for sec := int64(40); sec < 640; sec++ {
		burst = append(burst, pfGOP(sec, 100)...)
	}
	st.Publish(burst)
	pfExpectAllKept(t, vs, before, 100<<10, 5*time.Second, "ten minutes in one Update")
	// And the live tail goes on normally afterwards.
	pfWaitParked(t, st, vs)
	before = pfSnapshot(vs)
	for sec := int64(640); sec < 645; sec++ {
		st.Publish(pfGOP(sec, 100))
		time.Sleep(20 * time.Millisecond)
	}
	pfExpectAllKept(t, vs, before, 5*100*188, 3*time.Second, "live tail after the burst")
}

// TestParkedFollowersSurviveAReStampedSegmentLoop reproduces the burst measured
// in production on 2026-10-26: while the origin was down a master re-served
// one 4.09 s segment in a loop at disk speed and the relay's ffmpeg (+genpts,
// discontinuity correction) re-stamped every copy +4.09 s. The ring received
// 158 keyframe steps of +4.09 s about 12 ms apart — 27 minutes of PCR in ~4 s,
// every step a plausible cadence — with viewers draining at player rate. On
// master every parked viewer is dropped as "fell behind the ring"; with the
// re-seat none is, and the ring stays within its window.
func TestParkedFollowersSurviveAReStampedSegmentLoop(t *testing.T) {
	_, st, ts := pfSetup(t, 32, 40, 100)
	vs := pfStart(t, ts.Client(), ts.URL+"/live/5?prebuffer=2&c=v", 12, 3<<20)
	pfWaitParked(t, st, vs)
	before := pfSnapshot(vs)
	const step = 368100 // 4.09 s at 90 kHz
	tk := int64(40 * 90000)
	for i := 0; i < 158; i++ {
		b := tsfixture.KeyframePCR(0x101, tk, tk)
		for j := 0; j < 4000; j++ { // ≈ 0.75 MB per copy, published like the ingest does
			b = append(b, tsfixture.Fill(0x101)...)
		}
		for off := 0; off < len(b); off += 12032 {
			end := off + 12032
			if end > len(b) {
				end = len(b)
			}
			st.Publish(b[off:end])
		}
		tk += step
		time.Sleep(12 * time.Millisecond)
	}
	pfExpectAllKept(t, vs, before, 64<<10, 10*time.Second, "re-stamped segment loop")
	pfWaitParked(t, st, vs) // the viewers drain what the ring kept; none may be closed on the way
	if bytes, span, _ := st.Hub.RingStats(); span > 40_000 || bytes > 40*4001*188 {
		t.Fatalf("the ring retains more than its window after the burst: %d ms, %d bytes", span, bytes)
	}
	before = pfSnapshot(vs)
	sec := tk/90000 + 1
	for i := int64(0); i < 5; i++ {
		st.Publish(pfGOP(sec+i, 100))
		time.Sleep(20 * time.Millisecond)
	}
	pfExpectAllKept(t, vs, before, 5*100*188, 5*time.Second, "live tail after the loop")
}
