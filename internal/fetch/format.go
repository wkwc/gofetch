package fetch

import "fmt"

// humanBytes returns a short IEC-formatted byte count.
func humanBytes(n int64) string {
	const k = 1024
	if n <= 0 {
		return fmt.Sprintf("%d B", n)
	}
	switch {
	case n < k:
		return fmt.Sprintf("%d B", n)
	case n < k*k:
		return fmt.Sprintf("%.1f KB", float64(n)/k)
	case n < k*k*k:
		return fmt.Sprintf("%.1f MB", float64(n)/k/k)
	default:
		return fmt.Sprintf("%.2f GB", float64(n)/k/k/k)
	}
}
