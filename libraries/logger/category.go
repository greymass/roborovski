package logger

func validateCategory(category string) bool {
	if category == "" {
		return false
	}
	for _, r := range category {
		if r >= 'A' && r <= 'Z' {
			return false
		}
	}
	return true
}
