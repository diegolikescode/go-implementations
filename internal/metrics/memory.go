package metrics

import (
	"fmt"
	"runtime"
)

type MemObserver struct {
	maxAlloc uint64
}

func NewMemObserver() *MemObserver {
	return &MemObserver{maxAlloc: 0}
}

func (m *MemObserver) PrintMemoryUsage(label string) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	if m.maxAlloc < m.bToMb(ms.Alloc) {
		m.maxAlloc = m.bToMb(ms.Alloc)
	}

	fmt.Printf("\n====Memory Stats: %s====\n", label)
	fmt.Printf("Alloc: = %vMB", m.bToMb(ms.Alloc))
	fmt.Printf("\tMaxAlloc = %v", m.maxAlloc)
	fmt.Printf("\tTotalAlloc = %vMB", m.bToMb(ms.TotalAlloc))
	fmt.Printf("\tOS Usage = %vMB", m.bToMb(ms.Sys))
	fmt.Printf("\tNumGC = %v", ms.NumGC)
	fmt.Printf("\tGoroutines = %v\n", runtime.NumGoroutine())
}

func (m *MemObserver) bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
