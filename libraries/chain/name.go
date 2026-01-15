package chain


func charToSymbol(c byte) uint64 {
	if c >= 'a' && c <= 'z' {
		return uint64((c - 'a') + 6)
	}
	if c >= '1' && c <= '5' {
		return uint64((c - '1') + 1)
	}
	return uint64(0)
}

func symbolToChar(s byte) byte {
	if s >= 6 && s <= 31 {
		return byte(s - 6 + 'a')
	}
	if s > 0 && s <= 5 {
		return byte(s + '1' - 1)
	}
	if s == 0 {
		return byte('.')
	}
	return byte('@') // shouldn't reach
}

func StringToName(str string) uint64 {
	name := uint64(0)
	i := 0
	for ; i < 12 && len(str) > i; i++ {
		name |= (charToSymbol(str[i]) & 0x1F) << (64 - 5*(i+1))
	}
	if i == 12 && len(str) > 12 {
		name |= charToSymbol(str[12]) & 0x0F
	}
	return name
}

func NameToString(name uint64) string {
	buf := make([]byte, 0, 13) // Pre-allocate capacity for max name length
	for i := 0; i < 12; i++ {
		shfname := (name >> (64 - 5*(i+1)))
		buf = append(buf, symbolToChar(byte(shfname&0x1F)))
	}
	buf = append(buf, symbolToChar(byte(name&0x0F)))

	for len(buf) > 0 && buf[len(buf)-1] == '.' {
		buf = buf[:len(buf)-1]
	}
	return string(buf)
}
