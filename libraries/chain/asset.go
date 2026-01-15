package chain

import (
	"errors"
	"strconv"
	"strings"
)

type Asset struct {
	Amount int64
	Symbol uint64
}

func SymbolPrecision(symbol uint64) uint8 {
	return uint8(symbol & 0xFF)
}

func SymbolCode(symbol uint64) uint64 {
	return symbol >> 8
}

func SymbolCodeToString(code uint64) string {
	if code == 0 {
		return ""
	}
	var buf [7]byte
	n := 0
	for code > 0 && n < 7 {
		buf[n] = byte(code & 0xFF)
		code >>= 8
		n++
	}
	return string(buf[:n])
}

func StringToSymbolCode(s string) uint64 {
	var code uint64
	for i := len(s) - 1; i >= 0; i-- {
		code <<= 8
		code |= uint64(s[i])
	}
	return code
}

func NewSymbol(precision uint8, code uint64) uint64 {
	return (code << 8) | uint64(precision)
}

func NewSymbolFromString(precision uint8, name string) uint64 {
	return NewSymbol(precision, StringToSymbolCode(name))
}

func (a Asset) String() string {
	precision := SymbolPrecision(a.Symbol)
	code := SymbolCode(a.Symbol)
	symbolName := SymbolCodeToString(code)

	if precision == 0 {
		return strconv.FormatInt(a.Amount, 10) + " " + symbolName
	}

	negative := a.Amount < 0
	amount := a.Amount
	if negative {
		amount = -amount
	}

	var divisor int64 = 1
	for i := uint8(0); i < precision; i++ {
		divisor *= 10
	}

	whole := amount / divisor
	frac := amount % divisor

	fracStr := strconv.FormatInt(frac, 10)
	for len(fracStr) < int(precision) {
		fracStr = "0" + fracStr
	}

	var result string
	if negative {
		result = "-" + strconv.FormatInt(whole, 10) + "." + fracStr
	} else {
		result = strconv.FormatInt(whole, 10) + "." + fracStr
	}

	return result + " " + symbolName
}

func ParseAsset(s string) (Asset, error) {
	parts := strings.Split(s, " ")
	if len(parts) != 2 {
		return Asset{}, errors.New("invalid asset string: expected '<amount> <symbol>'")
	}

	amountStr := parts[0]
	symbolName := parts[1]

	negative := false
	if strings.HasPrefix(amountStr, "-") {
		negative = true
		amountStr = amountStr[1:]
	}

	var precision uint8
	var amountRaw string

	if dotIdx := strings.Index(amountStr, "."); dotIdx >= 0 {
		precision = uint8(len(amountStr) - dotIdx - 1)
		amountRaw = amountStr[:dotIdx] + amountStr[dotIdx+1:]
	} else {
		precision = 0
		amountRaw = amountStr
	}

	amount, err := strconv.ParseInt(amountRaw, 10, 64)
	if err != nil {
		return Asset{}, errors.New("invalid asset amount: " + err.Error())
	}

	if negative {
		amount = -amount
	}

	symbol := NewSymbolFromString(precision, symbolName)

	return Asset{Amount: amount, Symbol: symbol}, nil
}

func NewAsset(amount int64, symbol uint64) Asset {
	return Asset{Amount: amount, Symbol: symbol}
}
