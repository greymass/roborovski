package main

import (
	"fmt"
	"os"

	"github.com/greymass/roborovski/libraries/tracereader"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: inspect <block_num> <traces_dir> [count]")
		os.Exit(1)
	}

	var blockNum uint32
	fmt.Sscanf(os.Args[1], "%d", &blockNum)
	tracesDir := os.Args[2]

	count := 500 // full stride
	if len(os.Args) >= 4 {
		fmt.Sscanf(os.Args[3], "%d", &count)
	}

	conf := &tracereader.Config{
		Debug:  false,
		Stride: 500,
		Dir:    tracesDir,
	}

	fmt.Printf("Analyzing blocks %d to %d\n", blockNum, blockNum+uint32(count)-1)

	rawBlocks, err := tracereader.GetRawBlocksWithMetadata(blockNum, count, conf)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	truncated := 0
	valid := 0
	sizeHistogram := make(map[int]int)

	for _, rb := range rawBlocks {
		sizeHistogram[len(rb.RawBytes)]++

		if len(rb.RawBytes) == 0 {
			continue
		}

		data := rb.RawBytes
		pos := 0

		variant, n := decodeVarint(data[pos:])
		pos += n

		if variant != 2 || pos+149 > len(data) {
			continue
		}

		pos += 32 + 4 + 32 + 4 + 8 + 32 + 32 + 4 // skip to tx section

		txVariant, n := decodeVarint(data[pos:])
		pos += n
		txCount, n := decodeVarint(data[pos:])
		pos += n

		if txCount > 0 && txVariant == 1 && pos+32 < len(data) {
			pos += 32 // TX ID
			actVariant, n := decodeVarint(data[pos:])
			pos += n
			actCount, n := decodeVarint(data[pos:])
			pos += n

			if actCount > 0 && actVariant == 0 {
				pos += decodeVarintSize(data[pos:]) // ordinal
				pos += decodeVarintSize(data[pos:]) // creator ordinal
				pos += decodeVarintSize(data[pos:]) // closest unnotified
				pos += 24 // 3 uint64s

				authSeqCount, n := decodeVarint(data[pos:])
				pos += n

				needed := authSeqCount * 16
				remaining := len(data) - pos

				if needed > uint32(remaining) {
					truncated++
				} else {
					valid++
				}
			}
		}
	}

	fmt.Printf("\nResults:\n")
	fmt.Printf("  Valid blocks: %d\n", valid)
	fmt.Printf("  Truncated blocks: %d\n", truncated)
	fmt.Printf("  Total: %d\n", len(rawBlocks))

	fmt.Printf("\nBlock size histogram:\n")
	for size, count := range sizeHistogram {
		if count > 1 {
			fmt.Printf("  %d bytes: %d blocks\n", size, count)
		}
	}
}

func decodeVarint(data []byte) (uint32, int) {
	var result uint32
	var shift uint
	var pos int
	for pos < len(data) {
		b := data[pos]
		pos++
		result |= uint32(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return result, pos
}

func decodeVarintSize(data []byte) int {
	pos := 0
	for pos < len(data) {
		if data[pos]&0x80 == 0 {
			return pos + 1
		}
		pos++
	}
	return pos
}
