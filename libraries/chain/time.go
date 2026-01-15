package chain

import (
	"time"
)

func Uint32ToTime(t uint32) string {
	d := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	d = d.Add(time.Duration(uint64(t) * MSINTERVAL * 1000000))
	return d.Format("2006-01-02T15:04:05.000")
}

func TimeToUint32(t string) uint32 {
	if t == "" {
		return 0
	}
	if last := len(t) - 1; last >= 0 && t[last] == 'Z' {
		t = t[:last]
	}
	pt, err := time.Parse("2006-01-02T15:04:05.000", t)
	if err != nil {
		panic(err) // Caller should use enforce pattern
	}
	ptd := pt.Sub(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))
	return uint32(ptd.Milliseconds() / MSINTERVAL)
}
