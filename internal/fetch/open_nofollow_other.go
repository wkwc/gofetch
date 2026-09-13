//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package fetch

import "os"

// openOutputFile is the portability fallback for platforms without a portable
// O_NOFOLLOW flag in the standard library. allocateFileWriter still rejects an
// existing final symlink before this call.
func openOutputFile(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag, perm)
}
