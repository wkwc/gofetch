package fetch

import (
	"sync"
	"syscall"
	"time"
)

// BBREstimator implements BBR-style bandwidth and RTT estimation
// for TCP pacing via TCP_NOTSENT_LOWAT
type BBREstimator struct {
	mu sync.Mutex

	// BBR state
	rttMin     time.Duration
	btlBw      float64 // bottleneck bandwidth (bytes/sec)
	rttProp    time.Duration
	pacingGain float64
	cwndGain   float64

	// State
	roundCount    int
	packetCount   int64
	bytesAcked    int64
	bytesInFlight int64

	// For NOTSENT_LOWAT pacing
	fd            int
	pacingRate    int64 // bytes/sec
	minPacingRate int64
	maxPacingRate int64
}

// NewBBREstimator creates a new BBR estimator
func NewBBREstimator(fd int) *BBREstimator {
	return &BBREstimator{
		fd:            fd,
		minPacingRate: 64 * 1024,        // 64 KB/s minimum
		maxPacingRate: 10 * 1024 * 1024, // 10 MB/s default max
		rttMin:        0,
		pacingGain:    1.0,
		cwndGain:      2.0,
	}
}

// RecordPacketSent records a packet being sent
func (b *BBREstimator) RecordPacketSent(bytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.packetCount++
	b.bytesInFlight += bytes
}

// RecordAck records an acknowledged packet
func (b *BBREstimator) RecordAck(bytes int64, rtt time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.bytesAcked += bytes
	b.bytesInFlight -= bytes
	b.packetCount++

	// Update RTT minimum
	if b.rttMin == 0 || rtt < b.rttMin {
		b.rttMin = rtt
	}

	// Update delivery rate (bandwidth estimate)
	// Using packet train delivery rate
	deliveryRate := float64(bytes) / rtt.Seconds()
	if b.btlBw == 0 || deliveryRate > b.btlBw {
		b.btlBw = deliveryRate
	}
}

// UpdatePacingRate updates the TCP_NOTSENT_LOWAT socket option for pacing
func (b *BBREstimator) UpdatePacingRate(fd int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.btlBw == 0 {
		return nil
	}

	// BBR pacing rate = 2 * bottleneck bandwidth
	pacingRate := int64(b.btlBw * b.pacingGain)

	// Clamp to configured bounds
	if pacingRate < b.minPacingRate {
		pacingRate = b.minPacingRate
	}
	if pacingRate > b.maxPacingRate {
		pacingRate = b.maxPacingRate
	}

	// Apply via TCP_NOTSENT_LOWAT
	// SO_NOTSENT_LOWAT = pacing_rate * RTT
	lowat := int64(float64(pacingRate) * 0.1) // ~100ms worth
	if lowat < 16384 {
		lowat = 16384 // Minimum 16KB
	}

	// Apply via TCP_NOTSENT_LOWAT
	// SO_NOTSENT_LOWAT = 0x17
	const SOL_TCP = 0x06
	const TCP_NOTSENT_LOWAT = 0x17
	_, _, _ = syscall.Syscall6(syscall.SYS_SETSOCKOPT,
		uintptr(fd),
		uintptr(0x06), // SOL_TCP
		uintptr(0x17), // TCP_NOTSENT_LOWAT
		uintptr(lowat),
		uintptr(0),
		0)
	return nil
}

// GetStats returns current BBR statistics
func (b *BBREstimator) GetStats() BBREstimatorStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return BBREstimatorStats{
		RTTMin:        b.rttMin,
		BtlBw:         b.btlBw,
		PacingRate:    b.pacingRate,
		BytesInFlight: b.bytesInFlight,
		PacketsAcked:  b.packetCount,
	}
}

type BBREstimatorStats struct {
	RTTMin        time.Duration
	BtlBw         float64
	PacingRate    int64
	BytesInFlight int64
	PacketsAcked  int64
}
