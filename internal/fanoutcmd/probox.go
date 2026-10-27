// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package fanoutcmd

import (
	"fmt"
	"runtime/debug"
)

// Build stamp of a patched build (the "ring-resync" patch on top of the release).
//
// Deliberately, -version keeps printing the EXACT release version (0.15.5): it is
// what the panel compares (FanoutBinaryCommand: the xc_fanout.version sidecar
// against the tag resolved by lb_release_pin; the version whitelist of the
// `mejoras` module), and a suffix there would make the hourly cron reinstall the
// stock binary or switch fanout supervision off across the cluster. The stamp
// goes to -build-info, the boot log and `features` of /monitors/state
// (`probox_ring_resync`), the channel meant for a newer or older panel to
// discover what this binary can do.
const (
	ProBoxBuild = "probox.1"
	ProBoxPatch = "ring-resync"
)

// proboxBuildInfo describes the build: "<release>+probox.N <patch> (commit …)".
func proboxBuildInfo(upstream string) string {
	rev, dirty := "", ""
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				rev = s.Value
			case "vcs.modified":
				if s.Value == "true" {
					dirty = "-dirty"
				}
			}
		}
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if rev == "" {
		rev = "sin-vcs"
	}
	return fmt.Sprintf("%s+%s %s (commit %s%s)", upstream, ProBoxBuild, ProBoxPatch, rev, dirty)
}
