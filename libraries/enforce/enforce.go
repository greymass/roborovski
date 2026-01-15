package enforce

import (
	"math"

	"github.com/greymass/roborovski/libraries/logger"
)

func init() {
	CheckCompiler()
}

func ENFORCE(query interface{}, args ...interface{}) {
	switch t := query.(type) {
	case bool:
		{
			if t == false {
				logger.Printf("enforce", "ENFORCE: %v", args)
				panic(0)
			}
		}
	case error:
		{
			if t != nil {
				logger.Printf("enforce", "ENFORCE: %v", args)
				panic(t)
			}
		}
	}
}

func CheckCompiler() {
	myint := int(math.MaxInt64) // Shouldn't compile on a 32 bit system.
	myint64 := int64(math.MaxInt64)
	ENFORCE(uint64(myint) == uint64(myint64), "Must be on 64 bit system.")
}
