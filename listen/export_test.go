package listen

// SetForTest exposes the metrics update to the external test package.
func SetForTest(m *Metrics, f func(*Counters)) { m.set(f) }
