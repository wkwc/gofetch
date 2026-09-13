//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package fetch

import (
	"os"
	"syscall"
)

// openOutputFile atomically rejects a symbolic-link final path component.
// Lstat alone is vulnerable to a replacement between the check and open;
// O_NOFOLLOW makes the kernel enforce this invariant. Parent directories are
// intentionally caller-controlled paths, as is conventional for a CLI output.
func openOutputFile(path string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flag|syscall.O_NOFOLLOW, perm)
}
