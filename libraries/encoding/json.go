package encoding

import (
	"encoding/json"
	"strconv"

	jsoniter "github.com/json-iterator/go"
)

var JSONiter = jsoniter.Config{
	EscapeHTML:              false,
	MarshalFloatWith6Digits: false,
	DisallowUnknownFields:   false,
	OnlyTaggedField:         false,
	ValidateJsonRawMessage:  false,
	CaseSensitive:           true,
	UseNumber:               true,
	SortMapKeys:             false,
}.Froze()

func MaybeGetInt64(numberish interface{}) (int64, bool) {
	var err error
	number := int64(0)
	jsonNum, ok := numberish.(json.Number)
	if ok {
		number, err = jsonNum.Int64()
		if err != nil {
			return number, false
		}
	} else {
		stringNum, ok := numberish.(string)
		if !ok {
			intNum, ok := numberish.(int64)
			if !ok {
				return number, false
			}
			number = int64(intNum)
		} else {
			number, err = strconv.ParseInt(stringNum, 10, 64)
			if err != nil {
				return number, false
			}
		}
	}
	return number, true
}
