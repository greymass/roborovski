package internal

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

var antelopeEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

func encodeDateHour(t time.Time) []byte {
	t = t.UTC()
	days := uint32(t.Sub(antelopeEpoch).Hours() / 24)
	hour := uint32(t.Hour())
	encoded := (days << 5) | hour
	buf := make([]byte, 3)
	buf[0] = byte(encoded >> 16)
	buf[1] = byte(encoded >> 8)
	buf[2] = byte(encoded)
	return buf
}

type DateRange struct {
	StartDateHour []byte
	EndDateHour   []byte
}

func parseDateParam(date, startDate, endDate string) (*DateRange, error) {
	if date != "" {
		return parseSingleDate(date)
	}
	if startDate != "" || endDate != "" {
		return parseDateRange(startDate, endDate)
	}
	return nil, nil
}

func parseSingleDate(date string) (*DateRange, error) {
	if strings.Contains(date, ":") {
		t, err := parseDateHourString(date)
		if err != nil {
			return nil, err
		}
		dh := encodeDateHour(t)
		return &DateRange{StartDateHour: dh, EndDateHour: dh}, nil
	}

	t, err := parseDateString(date)
	if err != nil {
		return nil, err
	}
	startDH := encodeDateHour(t)
	endDH := encodeDateHour(t.Add(23 * time.Hour))
	return &DateRange{StartDateHour: startDH, EndDateHour: endDH}, nil
}

func parseDateRange(startDate, endDate string) (*DateRange, error) {
	var startT, endT time.Time
	var err error

	if startDate != "" {
		startT, err = parseDateOrDateHour(startDate)
		if err != nil {
			return nil, fmt.Errorf("invalid start_date: %w", err)
		}
	}

	if endDate != "" {
		endT, err = parseDateOrDateHour(endDate)
		if err != nil {
			return nil, fmt.Errorf("invalid end_date: %w", err)
		}
		if !strings.Contains(endDate, ":") {
			endT = endT.Add(23 * time.Hour)
		}
	}

	if startDate != "" && endDate == "" {
		endT = startT.AddDate(1, 0, 0)
	}
	if endDate != "" && startDate == "" {
		startT = endT.AddDate(-1, 0, 0)
	}

	if endT.Sub(startT) > 366*24*time.Hour {
		return nil, fmt.Errorf("date range cannot exceed 366 days")
	}

	return &DateRange{
		StartDateHour: encodeDateHour(startT),
		EndDateHour:   encodeDateHour(endT),
	}, nil
}

func parseDateOrDateHour(s string) (time.Time, error) {
	if strings.Contains(s, ":") {
		return parseDateHourString(s)
	}
	return parseDateString(s)
}

func parseDateString(s string) (time.Time, error) {
	parts := strings.Split(s, "-")
	if len(parts) != 3 {
		return time.Time{}, fmt.Errorf("expected YYYY-MM-DD format")
	}
	year, err := strconv.Atoi(parts[0])
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid year")
	}
	month, err := strconv.Atoi(parts[1])
	if err != nil || month < 1 || month > 12 {
		return time.Time{}, fmt.Errorf("invalid month")
	}
	day, err := strconv.Atoi(parts[2])
	if err != nil || day < 1 || day > 31 {
		return time.Time{}, fmt.Errorf("invalid day")
	}
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), nil
}

func parseDateHourString(s string) (time.Time, error) {
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("expected YYYY-MM-DD:HH format")
	}
	t, err := parseDateString(parts[0])
	if err != nil {
		return time.Time{}, err
	}
	hour, err := strconv.Atoi(parts[1])
	if err != nil || hour < 0 || hour > 23 {
		return time.Time{}, fmt.Errorf("invalid hour")
	}
	return t.Add(time.Duration(hour) * time.Hour), nil
}
