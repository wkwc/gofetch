package fetch

import "strconv"

// humanBytes returns a short IEC-formatted byte count.
func humanBytes(n int64) string {
	const k = 1024
	if n <= 0 {
		return strconv.FormatInt(n, 10) + " B"
	}
	switch {
	case n < k:
		return strconv.FormatInt(n, 10) + " B"
	case n < k*k:
		return formatFloat(float64(n)/k) + " KB"
	case n < k*k*k:
		return formatFloat(float64(n)/k/k) + " MB"
	default:
		return formatFloat2(float64(n)/k/k/k) + " GB"
	}
}

// formatFloat formats a float with one decimal place without fmt.Sprintf.
func formatFloat(f float64) string {
	whole := int64(f)
	frac := int64((f - float64(whole)) * 10)
	return strconv.FormatInt(whole, 10) + "." + strconv.FormatInt(frac, 10)
}

// formatFloat2 formats a float with two decimal places without fmt.Sprintf.
func formatFloat2(f float64) string {
	whole := int64(f)
	frac := int64((f - float64(whole)) * 100)
	return strconv.FormatInt(whole, 10) + "." + pad2(frac)
}

// pad2 zero-pads an integer to 2 digits.
func pad2(n int64) string {
	if n < 10 {
		return "0" + strconv.FormatInt(n, 10)
	}
	return strconv.FormatInt(n, 10)
}
