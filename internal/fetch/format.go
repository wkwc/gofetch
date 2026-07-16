package fetch

import "strconv"

// humanBytes returns a short IEC-formatted byte count.
func humanBytes(n int64) string {
	const k = 1024
	if n < k {
		return strconv.FormatInt(n, 10) + " B"
	}
	switch {
	case n < k*k:
		return formatFixed1(float64(n)/k) + " KB"
	case n < k*k*k:
		return formatFixed1(float64(n)/k/k) + " MB"
	default:
		return formatFixed2(float64(n)/k/k/k) + " GB"
	}
}

// formatFixed1 formats a float with one decimal place, rounding to nearest.
func formatFixed1(f float64) string {
	// Round to one decimal by adding 0.05 (half of 0.1).
	f += 0.05
	whole := int64(f)
	frac := int64((f - float64(whole)) * 10)
	return strconv.FormatInt(whole, 10) + "." + strconv.FormatInt(frac, 10)
}

// formatFixed2 formats a float with two decimal places, rounding to nearest.
func formatFixed2(f float64) string {
	// Round to two decimals by adding 0.005.
	f += 0.005
	whole := int64(f)
	frac := int64((f - float64(whole)) * 100)
	if frac < 10 {
		return strconv.FormatInt(whole, 10) + ".0" + strconv.FormatInt(frac, 10)
	}
	return strconv.FormatInt(whole, 10) + "." + strconv.FormatInt(frac, 10)
}
