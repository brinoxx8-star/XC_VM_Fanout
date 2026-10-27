// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package server

// serveLive tests for the ring-resync change: what was measured on production
// load balancers (100 viewers parked during an origin outage; when the signal
// comes back the ring advances more than its window before any of them reads
// and all get EOF with "fell behind the ring") must end in a re-seat, not an
// EOF; and a client that really cannot keep up must still be dropped.

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// rsViewer is a synthetic live-TS viewer: it reads its response continuously
// and counts bytes; done is closed when the daemon closes the connection (or
// on error).
type rsViewer struct {
	got  atomic.Int64
	done chan struct{}
	err  error
	// pause > 0: the reader stops (does not read the socket) for that long once
	// it has received pauseAfter bytes — a player that stops draining.
	pause      time.Duration
	pauseAfter int64
	paused     atomic.Bool
	// rate > 0: the reader drains at most rate bytes per second (a real player
	// reads at the video's pace, not at link speed).
	rate int64
}

// rsStartRated is rsStart with readers limited to rate bytes/s.
func rsStartRated(t *testing.T, client *http.Client, url string, n int, rate int64) []*rsViewer {
	t.Helper()
	vs := rsStartOpts(t, client, url, n, 0, 0, rate)
	return vs
}

func (v *rsViewer) finished() bool {
	select {
	case <-v.done:
		return true
	default:
		return false
	}
}

func rsStart(t *testing.T, client *http.Client, url string, n int, pause time.Duration, pauseAfter int64) []*rsViewer {
	t.Helper()
	return rsStartOpts(t, client, url, n, pause, pauseAfter, 0)
}

func rsStartOpts(t *testing.T, client *http.Client, url string, n int, pause time.Duration, pauseAfter int64, rate int64) []*rsViewer {
	t.Helper()
	vs := make([]*rsViewer, n)
	ready := make(chan struct{}, n)
	// The viewers hang off the test's context: at the end they are cancelled so
	// the test server can close (Close waits for open connections; a TS session
	// never ends on its own).
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		for _, v := range vs {
			<-v.done
		}
	})
	for i := range vs {
		v := &rsViewer{done: make(chan struct{}), pause: pause, pauseAfter: pauseAfter, rate: rate}
		vs[i] = v
		go func(v *rsViewer) {
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
				if v.rate > 0 {
					// Sleep as needed to stay under rate bytes/s cumulatively.
					if ahead := time.Duration(float64(v.got.Load())/float64(v.rate)*float64(time.Second)) - time.Since(start); ahead > 0 {
						time.Sleep(ahead)
					}
				}
				if v.pause > 0 && !v.paused.Load() && v.got.Load() >= v.pauseAfter {
					v.paused.Store(true)
					time.Sleep(v.pause)
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

// rsWaitParked waits until every viewer has drained the ring (their counters
// stop moving) and is parked at the edge.
func rsWaitParked(t *testing.T, st *Stream, vs []*rsViewer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	last := make([]int64, len(vs))
	stable := 0
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
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
		if stable++; stable >= 4 { // 200 ms without movement with everyone attached
			return
		}
	}
	t.Fatalf("the viewers never parked")
}

// rsSnapshot copies the byte counters.
func rsSnapshot(vs []*rsViewer) []int64 {
	out := make([]int64, len(vs))
	for i, v := range vs {
		out[i] = v.got.Load()
	}
	return out
}

// rsExpectAllReceive fails if within timeout any viewer got EOF or did not
// receive at least min bytes more than in before.
func rsExpectAllReceive(t *testing.T, vs []*rsViewer, before []int64, min int64, timeout time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok := true
		for i, v := range vs {
			if v.finished() {
				t.Fatalf("%s: viewer %d closed (err=%v, bytes=%d)", what, i, v.err, v.got.Load())
			}
			if v.got.Load()-before[i] < min {
				ok = false
			}
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

// rsExpectAllClosed fails if within timeout any viewer is still connected.
func rsExpectAllClosed(t *testing.T, vs []*rsViewer, timeout time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		open := 0
		for _, v := range vs {
			if !v.finished() {
				open++
			}
		}
		if open == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %d of %d viewers still connected", what, open, len(vs))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// rsGOP is a one-second GOP opening at sec: a keyframe with PCR and pad filler
// packets (pad=100 ≈ 19 KB, pad=5500 ≈ 1 MB).
func rsGOP(sec int64, pad int) []byte {
	b := tsfixture.KeyframePCR(0x101, sec*90000, sec*90000)
	for i := 0; i < pad; i++ {
		b = append(b, tsfixture.Fill(0x101)...)
	}
	return b
}

// rsBurst is the burst: secs seconds of coherent video (PCR advancing one
// second per GOP) from from, in ONE byte slice.
func rsBurst(from, secs int64, pad int) []byte {
	var b []byte
	for sec := from; sec < from+secs; sec++ {
		b = append(b, rsGOP(sec, pad)...)
	}
	return b
}

// rsSetup brings up a Manager with a ringSec ring, a stream with secs seconds
// published and the HTTP server. smallBuffers caps the send buffer of every
// accepted connection (64 KB): a client that stops reading then blocks the
// writer as soon as it stops draining, not after megabytes of kernel cushion.
func rsSetup(t *testing.T, ringSec int, secs int64, pad int, smallBuffers bool) (*Manager, *Stream, *httptest.Server) {
	t.Helper()
	// hls_target_sec 1 with smallBuffers: the HLS view floors the ring at two
	// segments (2 × hls_target_sec), which with the default 6 s would turn a
	// short test window into 12 s.
	hlsTarget := 6.0
	if smallBuffers {
		hlsTarget = 1
	}
	mgr := NewManager(10<<20, int64(ringSec)*1000, hlsTarget, 6, time.Hour)
	mgr.viewerIdleNS.Store(0)
	st := mgr.GetOrCreate("5")
	st.Publish(tsfixture.PAT(0x100))
	st.Publish(tsfixture.PMT(0x100, 0x101))
	for sec := int64(0); sec < secs; sec++ {
		st.Publish(rsGOP(sec, pad))
	}
	ts := httptest.NewUnstartedServer(mgr.ClientHandler())
	if smallBuffers {
		ts.Listener = rsSmallSendBuf{ts.Listener}
	}
	ts.Start()
	t.Cleanup(ts.Close)
	return mgr, st, ts
}

// rsSmallSendBuf is a listener whose accepted connections get a 64 KB SO_SNDBUF.
type rsSmallSendBuf struct{ net.Listener }

func (l rsSmallSendBuf) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		if raw, err := tc.SyscallConn(); err == nil {
			_ = raw.Control(func(fd uintptr) {
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, 64<<10)
			})
		}
	}
	return c, nil
}

// TestResyncBurstOfMinutesInOneUpdate (P3, the measured case): 60 parked
// viewers; the signal comes back as a dump of 10 minutes of coherent video
// that the ring runs through in a single Update (< 1 s of wall-clock time), so
// everyone's block is pruned before anyone reads. Before: 60 × EOF "fell
// behind the ring". After: 60 re-seats, nobody closed, and all keep receiving
// the live tail that follows.
func TestResyncBurstOfMinutesInOneUpdate(t *testing.T) {
	mgr, st, ts := rsSetup(t, 32, 40, 100, false)
	vs := rsStart(t, ts.Client(), ts.URL+"/live/5?prebuffer=6&c=v", 60, 0, 0)
	rsWaitParked(t, st, vs)

	before := rsSnapshot(vs)
	st.Publish(rsBurst(40, 600, 100)) // ten minutes of video in one Update
	rsExpectAllReceive(t, vs, before, 100<<10, 5*time.Second, "burst in one Update")
	if got := st.resyncs.Load(); got != int64(len(vs)) {
		t.Fatalf("re-seats = %d, want %d (one per parked viewer)", got, len(vs))
	}
	if mgr.resyncs.Load() != st.resyncs.Load() {
		t.Fatalf("global counter %d ≠ stream's %d", mgr.resyncs.Load(), st.resyncs.Load())
	}

	// After the burst the live tail goes on at normal pace: nobody drops.
	rsWaitParked(t, st, vs)
	before = rsSnapshot(vs)
	for sec := int64(640); sec < 645; sec++ {
		st.Publish(rsGOP(sec, 100))
		time.Sleep(20 * time.Millisecond)
	}
	rsExpectAllReceive(t, vs, before, 5*100*188, 3*time.Second, "live tail after the burst")
	if st.resyncs.Load() != int64(len(vs)) {
		t.Fatalf("extra re-seats during the normal live tail: %d", st.resyncs.Load())
	}
	if js := st.status(); js.Resyncs != int64(len(vs)) {
		t.Fatalf("/streams/<id> resyncs=%d", js.Resyncs)
	}
}

// TestResyncOffReproducesMassDrop witnesses the "before": with viewer_resync
// off the same resume closes every parked viewer at once.
func TestResyncOffReproducesMassDrop(t *testing.T) {
	mgr, st, ts := rsSetup(t, 32, 40, 100, false)
	mgr.viewerResync.Store(false)
	vs := rsStart(t, ts.Client(), ts.URL+"/live/5?prebuffer=6&c=v", 30, 0, 0)
	rsWaitParked(t, st, vs)
	st.Publish(rsBurst(40, 600, 100))
	rsExpectAllClosed(t, vs, 5*time.Second, "without re-seat")
	if st.resyncs.Load() != 0 {
		t.Fatalf("re-seats with the feature off: %d", st.resyncs.Load())
	}
}

// TestResyncBurstInIngestChunks: the same burst chunked the way the real
// ingest publishes it (12,032 bytes per Publish, no pause). The followers
// contend for the lock with the producer; re-seats or not, nobody gets EOF.
func TestResyncBurstInIngestChunks(t *testing.T) {
	_, st, ts := rsSetup(t, 32, 40, 100, false)
	vs := rsStart(t, ts.Client(), ts.URL+"/live/5?prebuffer=6&c=v", 40, 0, 0)
	rsWaitParked(t, st, vs)
	before := rsSnapshot(vs)
	burst := rsBurst(40, 600, 100)
	const chunk = 12032
	for off := 0; off < len(burst); off += chunk {
		end := off + chunk
		if end > len(burst) {
			end = len(burst)
		}
		st.Publish(burst[off:end])
	}
	rsExpectAllReceive(t, vs, before, 100<<10, 5*time.Second, "chunked burst")
	rsWaitParked(t, st, vs)
	before = rsSnapshot(vs)
	for sec := int64(640); sec < 645; sec++ {
		st.Publish(rsGOP(sec, 100))
		time.Sleep(20 * time.Millisecond)
	}
	rsExpectAllReceive(t, vs, before, 5*100*188, 3*time.Second, "live tail after the chunked burst")
}

// TestResyncProducerRestartWithBurst (P4): the origin restarts with a new PCR
// base (near zero: a backward step) and its first delivery arrives as a burst.
// The backward step is one cadence (harmless); the burst prunes; nobody drops.
func TestResyncProducerRestartWithBurst(t *testing.T) {
	_, st, ts := rsSetup(t, 32, 40, 100, false)
	vs := rsStart(t, ts.Client(), ts.URL+"/live/5?prebuffer=6&c=v", 30, 0, 0)
	rsWaitParked(t, st, vs)
	before := rsSnapshot(vs)
	st.Publish(tsfixture.PAT(0x100))
	st.Publish(tsfixture.PMT(0x100, 0x101))
	st.Publish(rsBurst(1, 300, 100)) // new instance: PCR from 1 s, five minutes at once
	rsExpectAllReceive(t, vs, before, 100<<10, 5*time.Second, "restart with burst")
	rsWaitParked(t, st, vs)
	before = rsSnapshot(vs)
	for sec := int64(301); sec < 306; sec++ {
		st.Publish(rsGOP(sec, 100))
		time.Sleep(20 * time.Millisecond)
	}
	rsExpectAllReceive(t, vs, before, 5*100*188, 3*time.Second, "live tail after the restart")
}

// TestResyncForwardJumpAfterGap (P1 seen from the server): a gap with no data
// and the signal returns with the source clock 45 s ahead. With the ring
// clock's window cap no re-seat is even needed: nobody drops.
func TestResyncForwardJumpAfterGap(t *testing.T) {
	_, st, ts := rsSetup(t, 32, 40, 100, false)
	vs := rsStart(t, ts.Client(), ts.URL+"/live/5?prebuffer=6&c=v", 30, 0, 0)
	rsWaitParked(t, st, vs)
	before := rsSnapshot(vs)
	head := append(tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101)...)
	st.Publish(append(head, rsGOP(39+45, 100)...))
	for sec := int64(85); sec < 90; sec++ {
		st.Publish(rsGOP(sec, 100))
		time.Sleep(20 * time.Millisecond)
	}
	rsExpectAllReceive(t, vs, before, 6*100*188, 3*time.Second, "45 s jump")
	if st.resyncs.Load() != 0 {
		t.Fatalf("a jump within maxCadence needed %d re-seats: the window cap did not act", st.resyncs.Load())
	}
}

// TestResyncBudgetDropsAFlappingViewer: with a cap of 2 re-seats per minute the
// third burst in a row closes the viewer as "fell behind".
func TestResyncBudgetDropsAFlappingViewer(t *testing.T) {
	mgr, st, ts := rsSetup(t, 32, 40, 100, false)
	mgr.viewerResyncMaxPerMin.Store(2)
	vs := rsStart(t, ts.Client(), ts.URL+"/live/5?prebuffer=6&c=v", 5, 0, 0)
	rsWaitParked(t, st, vs)
	from := int64(40)
	for round := 1; round <= 2; round++ {
		before := rsSnapshot(vs)
		st.Publish(rsBurst(from, 300, 100))
		from += 300
		rsExpectAllReceive(t, vs, before, 100<<10, 5*time.Second, "burst within the cap")
		rsWaitParked(t, st, vs)
		time.Sleep(resyncEpisodeGap + 200*time.Millisecond) // distinct episodes
	}
	st.Publish(rsBurst(from, 300, 100))
	rsExpectAllClosed(t, vs, 5*time.Second, "third burst with a cap of 2")
	if got := st.resyncs.Load(); got != int64(2*len(vs)) {
		t.Fatalf("re-seats = %d, want %d", got, 2*len(vs))
	}
}

// TestResyncSlowClientStillDropped: a client that stops draining its socket
// while the stream goes on in real time falls more than one window of
// WALL-CLOCK time behind: it is genuinely slow and is closed as always ("fell
// behind"), with no re-seat. A second viewer that does read is untouched.
func TestResyncSlowClientStillDropped(t *testing.T) {
	const ringSec = 3
	mgr, st, ts := rsSetup(t, ringSec, 4, 5500, true) // ≈ 1 MB GOPs: the socket buffers fill at once
	mgr.writeTimeout.Store(int64(60 * time.Second))   // the write deadline must not be what closes it

	// Small receive buffers: what the client does not read shows at the writer
	// as soon as it stops draining, not after megabytes of kernel cushion.
	dialer := &net.Dialer{Control: func(network, address string, c syscall.RawConn) error {
		return c.Control(func(fd uintptr) {
			_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 64<<10)
		})
	}}
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, n, a string) (net.Conn, error) {
		return dialer.DialContext(ctx, n, a)
	}}}

	fast := rsStart(t, ts.Client(), ts.URL+"/live/5?prebuffer=0&c=fast", 1, 0, 0)
	slow := rsStart(t, client, ts.URL+"/live/5?prebuffer=0&c=slow", 1, 7*time.Second, 512<<10)
	rsWaitParked(t, st, append(append([]*rsViewer{}, fast...), slow...))

	// Live tail in real time: one one-second GOP per wall-clock second.
	stop := make(chan struct{})
	go func() {
		sec := int64(4)
		tk := time.NewTicker(time.Second)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				st.Publish(rsGOP(sec, 5500))
				sec++
			}
		}
	}()
	defer close(stop)

	rsExpectAllClosed(t, slow, 20*time.Second, "cliente lento")
	if slow[0].err != nil {
		t.Fatalf("the slow client ended with an error instead of a clean EOF: %v", slow[0].err)
	}
	if fast[0].finished() {
		t.Fatalf("the fast viewer was closed (err=%v)", fast[0].err)
	}
	if st.resyncs.Load() != 0 {
		t.Fatalf("the slow client was re-seated %d times instead of dropped", st.resyncs.Load())
	}
}

// rsSegmentLoop reproduces the burst measured in production (a master re-sent
// the SAME 4.09 s segment from offset 0 in a loop at disk speed while the
// origin was down, and the relay's ffmpeg re-stamped each copy +4.09 s): copies
// copies of pad+1 packets, coherent PCR (+368,100 ticks per copy), published
// like the ingest does (12,032 B per Publish) with a pause of every between
// copies.
func rsSegmentLoop(st *Stream, fromTicks int64, copies, pad int, every time.Duration) int64 {
	const step = 368100 // 4,09 s a 90 kHz
	t := fromTicks
	const chunk = 12032
	for i := 0; i < copies; i++ {
		b := tsfixture.KeyframePCR(0x101, t, t)
		for j := 0; j < pad; j++ {
			b = append(b, tsfixture.Fill(0x101)...)
		}
		for off := 0; off < len(b); off += chunk {
			end := off + chunk
			if end > len(b) {
				end = len(b)
			}
			st.Publish(b[off:end])
		}
		t += step
		if every > 0 {
			time.Sleep(every)
		}
	}
	return t
}

// TestResyncMeasuredSegmentLoopBurst (P3 as measured): 158 copies of the same
// segment re-stamped +4.09 s every 12 ms (27 minutes of stream clock in ~2 s of
// wall-clock time; ~400 MB per relay on the node), with viewers draining at
// player rate (3 MB/s). Before: all "fell behind" at once. After: nobody gets
// EOF, they are re-seated and the ring never retains more than its window; when
// the live tail returns to normal pace they all carry on.
func TestResyncMeasuredSegmentLoopBurst(t *testing.T) {
	_, st, ts := rsSetup(t, 32, 40, 100, true)
	vs := rsStartRated(t, ts.Client(), ts.URL+"/live/5?prebuffer=2&c=v", 12, 3<<20)
	rsWaitParked(t, st, vs)
	before := rsSnapshot(vs)

	end := rsSegmentLoop(st, 40*90000, 158, 4000, 12*time.Millisecond) // ≈ 0.75 MB per copy, ≈ 120 MB
	rsExpectAllReceive(t, vs, before, 64<<10, 10*time.Second, "segment loop")
	if bytes, span, _ := st.Hub.RingStats(); span > 40_000 || bytes > 40*4001*188 {
		t.Fatalf("the ring retains more than its window after the burst: %d ms, %d bytes", span, bytes)
	}
	t.Logf("re-seats during the loop: %d (12 viewers)", st.resyncs.Load())
	if st.resyncs.Load() == 0 {
		t.Fatalf("no re-seat: the scenario never pruned the followers (adjust the pace)")
	}

	// Live tail at normal pace after the burst: nobody drops and all receive.
	rsWaitParked(t, st, vs)
	before = rsSnapshot(vs)
	sec := end/90000 + 1
	for i := int64(0); i < 5; i++ {
		st.Publish(rsGOP(sec+i, 100))
		time.Sleep(20 * time.Millisecond)
	}
	rsExpectAllReceive(t, vs, before, 5*100*188, 5*time.Second, "live tail after the loop")
}

// TestResyncMeasuredSegmentLoopBurstOffDropsAll witnesses the "before" for the
// same burst: with viewer_resync off, viewers at player rate are all dropped.
func TestResyncMeasuredSegmentLoopBurstOffDropsAll(t *testing.T) {
	mgr, st, ts := rsSetup(t, 32, 40, 100, true)
	mgr.viewerResync.Store(false)
	vs := rsStartRated(t, ts.Client(), ts.URL+"/live/5?prebuffer=2&c=v", 12, 3<<20)
	rsWaitParked(t, st, vs)
	rsSegmentLoop(st, 40*90000, 158, 4000, 12*time.Millisecond)
	rsExpectAllClosed(t, vs, 10*time.Second, "segment loop without re-seat")
}

// TestResyncParkedThroughLongOutage: a source gap LONGER than the ring window
// (5 s of silence with a 3 s window) followed by a burst does not make the
// parked viewer "slow": until it wakes it had nothing to read, and it is
// re-seated.
func TestResyncParkedThroughLongOutage(t *testing.T) {
	_, st, ts := rsSetup(t, 3, 6, 100, false)
	vs := rsStart(t, ts.Client(), ts.URL+"/live/5?prebuffer=1&c=v", 10, 0, 0)
	rsWaitParked(t, st, vs)
	time.Sleep(5 * time.Second) // the source is silent for longer than the window
	before := rsSnapshot(vs)
	st.Publish(rsBurst(6, 120, 100)) // and comes back with two minutes at once
	rsExpectAllReceive(t, vs, before, 16<<10, 5*time.Second, "burst after a long gap")
	if got := st.resyncs.Load(); got != int64(len(vs)) {
		t.Fatalf("re-seats = %d, want %d", got, len(vs))
	}
}
