package appendlog

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/zstd"
	"github.com/greymass/roborovski/libraries/logger"
)

const (
	BlocksPerMegaDownload = 1000000
	SlicesPerMegaDownload = 100
)

type SliceDownloader struct {
	baseURL        string
	storagePath    string
	parallel       int
	blocksPerSlice uint32
	client         *http.Client
	store          *SliceStore
}

func NewSliceDownloader(baseURL, storagePath string, parallel int, blocksPerSlice uint32, timeoutMinutes int, store *SliceStore) *SliceDownloader {
	if baseURL == "" {
		return nil
	}

	if !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}

	if timeoutMinutes <= 0 {
		timeoutMinutes = 30
	}

	return &SliceDownloader{
		baseURL:        baseURL,
		storagePath:    storagePath,
		parallel:       parallel,
		blocksPerSlice: blocksPerSlice,
		store:          store,
		client: &http.Client{
			Timeout: time.Duration(timeoutMinutes) * time.Minute,
		},
	}
}

func (d *SliceDownloader) sliceArchiveName(sliceNum uint32) string {
	startBlock := sliceNum*d.blocksPerSlice + 1
	endBlock := (sliceNum + 1) * d.blocksPerSlice
	return fmt.Sprintf("history_%010d-%010d.tar.zst", startBlock, endBlock)
}

func (d *SliceDownloader) megaArchiveName(megaNum uint32) string {
	startBlock := megaNum*BlocksPerMegaDownload + 1
	endBlock := (megaNum + 1) * BlocksPerMegaDownload
	return fmt.Sprintf("mega_%010d-%010d.tar.zst", startBlock, endBlock)
}

func (d *SliceDownloader) sliceDirName(sliceNum uint32) string {
	startBlock := sliceNum*d.blocksPerSlice + 1
	endBlock := (sliceNum + 1) * d.blocksPerSlice
	return fmt.Sprintf("history_%010d-%010d", startBlock, endBlock)
}

func (d *SliceDownloader) SliceNumForBlock(blockNum uint32) uint32 {
	if blockNum == 0 {
		return 0
	}
	return (blockNum - 1) / d.blocksPerSlice
}

func (d *SliceDownloader) isSliceFinalized(sliceNum uint32) bool {
	sliceDir := filepath.Join(d.storagePath, d.sliceDirName(sliceNum))
	dataLogPath := filepath.Join(sliceDir, "data.log")

	header, err := ReadDataLogHeader(dataLogPath)
	if err != nil {
		return false
	}

	return header.Finalized == 1
}

type sliceResult struct {
	sliceNum uint32
	success  bool
	notFound bool
	err      error
	info     *SliceInfo
}

func (d *SliceDownloader) DownloadSlices(ctx context.Context, startSlice uint32) error {
	sem := make(chan struct{}, d.parallel)
	results := make(chan sliceResult, d.parallel*2)

	var wg sync.WaitGroup
	stopCh := make(chan struct{})
	var stopOnce sync.Once
	cancelled := false
	var cancelMu sync.Mutex

	go func() {
		defer close(results)

		for sliceNum := startSlice; ; sliceNum++ {
			select {
			case <-ctx.Done():
				logger.Println("download", "Download cancelled, waiting for workers to finish...")
				cancelMu.Lock()
				cancelled = true
				cancelMu.Unlock()
				wg.Wait()
				logger.Println("download", "All download workers stopped")
				return
			case <-stopCh:
				wg.Wait()
				return
			default:
			}

			if d.isSliceFinalized(sliceNum) {
				results <- sliceResult{sliceNum: sliceNum, success: true}
				continue
			}

			megaNum := sliceNum / SlicesPerMegaDownload
			megaStartSlice := megaNum * SlicesPerMegaDownload

			if sliceNum == megaStartSlice {
				sem <- struct{}{}
				wg.Add(1)

				go func(mn uint32, startSl uint32) {
					startBlk := startSl*d.blocksPerSlice + 1
					endBlk := (startSl + SlicesPerMegaDownload) * d.blocksPerSlice
					defer wg.Done()
					defer func() { <-sem }()

					select {
					case <-ctx.Done():
						return
					case <-stopCh:
						return
					default:
					}

					result := d.downloadAndProcessMega(ctx, mn)
					if result.err == nil && !result.notFound {
						logger.Printf("download", "✓ Blocks %d-%d extracted (100 slices ready)", startBlk, endBlk)
						for i := uint32(0); i < SlicesPerMegaDownload; i++ {
							results <- sliceResult{sliceNum: startSl + i, success: true}
						}
						return
					}

					if result.notFound {
						logger.Printf("download", "Mega archive not found for blocks %d-%d, trying individual slices...", startBlk, endBlk)
					} else if result.err != nil {
						logger.Warning("Failed to download blocks %d-%d: %v", startBlk, endBlk, result.err)
					}

					for i := uint32(0); i < SlicesPerMegaDownload; i++ {
						results <- sliceResult{sliceNum: startSl + i, success: false}
					}
				}(megaNum, megaStartSlice)

				for i := uint32(0); i < SlicesPerMegaDownload; i++ {
					sliceNum++
				}
				sliceNum--
				continue
			}

			sem <- struct{}{}
			wg.Add(1)

			go func(sn uint32) {
				defer wg.Done()
				defer func() { <-sem }()

				select {
				case <-ctx.Done():
					return
				case <-stopCh:
					return
				default:
				}

				result := d.downloadAndProcess(ctx, sn)
				results <- result

				if result.notFound || result.err != nil {
					stopOnce.Do(func() { close(stopCh) })
				}
			}(sliceNum)
		}
	}()

	nextExpected := startSlice
	pending := make(map[uint32]*SliceInfo)
	var lastErr error
	var highestApplied uint32

	for result := range results {
		if result.notFound {
			startBlock := result.sliceNum*d.blocksPerSlice + 1
			if highestApplied > 0 {
				totalDownloaded := highestApplied - (startSlice * d.blocksPerSlice)
				slicesDownloaded := result.sliceNum - startSlice
				logger.Printf("download", "CDN complete: downloaded %d blocks (%d slices), up to block %d",
					totalDownloaded, slicesDownloaded, highestApplied)
			} else {
				logger.Printf("download", "No blocks available from CDN starting at block %d", startBlock)
			}
			continue
		}

		if result.err != nil {
			startBlock := result.sliceNum*d.blocksPerSlice + 1
			endBlock := (result.sliceNum + 1) * d.blocksPerSlice
			logger.Warning("Failed to download blocks %d-%d: %v", startBlock, endBlock, result.err)
			lastErr = result.err
			continue
		}

		if result.info != nil {
			pending[result.sliceNum] = result.info
		}

		for {
			info, ok := pending[nextExpected]
			if !ok {
				break
			}

			if info != nil {
				startBlock := nextExpected*d.blocksPerSlice + 1
				endBlock := info.EndBlock
				if err := d.applySlice(nextExpected, info); err != nil {
					logger.Warning("Failed to index blocks %d-%d: %v", startBlock, endBlock, err)
					lastErr = err
					break
				}
				logger.Printf("download", "✓ Blocks %d-%d indexed (LIB=%d)", startBlock, endBlock, info.EndBlock)
				highestApplied = info.EndBlock
			}

			delete(pending, nextExpected)
			nextExpected++
		}
	}

	cancelMu.Lock()
	wasCancelled := cancelled
	cancelMu.Unlock()

	if wasCancelled {
		return ctx.Err()
	}

	return lastErr
}

func (d *SliceDownloader) downloadAndProcess(ctx context.Context, sliceNum uint32) sliceResult {
	result := sliceResult{sliceNum: sliceNum}

	archiveName := d.sliceArchiveName(sliceNum)
	url := d.baseURL + archiveName

	startBlock := sliceNum*d.blocksPerSlice + 1
	endBlock := (sliceNum + 1) * d.blocksPerSlice
	logger.Printf("download", "→ Downloading blocks %d-%d...", startBlock, endBlock)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		result.err = fmt.Errorf("failed to create request: %w", err)
		return result
	}

	resp, err := d.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			result.err = fmt.Errorf("download cancelled: %w", ctx.Err())
		} else {
			result.err = fmt.Errorf("download failed: %w", err)
		}
		return result
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		result.notFound = true
		return result
	}

	if resp.StatusCode != http.StatusOK {
		result.err = fmt.Errorf("unexpected status: %d", resp.StatusCode)
		return result
	}

	tmpFile, err := os.CreateTemp("", "slice-*.tar.zst")
	if err != nil {
		result.err = fmt.Errorf("failed to create temp file: %w", err)
		return result
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		tmpFile.Close()
		result.err = fmt.Errorf("download copy failed: %w", err)
		return result
	}
	tmpFile.Close()

	info, err := d.extractVerifyAndRebuild(tmpPath, sliceNum)
	if err != nil {
		result.err = err
		return result
	}

	result.success = true
	result.info = info
	return result
}

func (d *SliceDownloader) extractVerifyAndRebuild(archivePath string, sliceNum uint32) (*SliceInfo, error) {
	sliceDir := filepath.Join(d.storagePath, d.sliceDirName(sliceNum))

	if err := os.MkdirAll(sliceDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create slice directory: %w", err)
	}

	file, err := os.Open(archivePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open archive: %w", err)
	}
	defer file.Close()

	zr := zstd.NewReader(file)
	defer zr.Close()

	tr := tar.NewReader(zr)

	var dataLogPath string
	var expectedHash string

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			os.RemoveAll(sliceDir)
			return nil, fmt.Errorf("tar read error: %w", err)
		}

		switch header.Name {
		case "data.log":
			dataLogPath = filepath.Join(sliceDir, "data.log")
			outFile, err := os.Create(dataLogPath)
			if err != nil {
				os.RemoveAll(sliceDir)
				return nil, fmt.Errorf("failed to create data.log: %w", err)
			}
			if _, err := io.Copy(outFile, tr); err != nil {
				outFile.Close()
				os.RemoveAll(sliceDir)
				return nil, fmt.Errorf("failed to write data.log: %w", err)
			}
			outFile.Close()

		case "sha256.txt":
			hashBytes, err := io.ReadAll(tr)
			if err != nil {
				os.RemoveAll(sliceDir)
				return nil, fmt.Errorf("failed to read sha256.txt: %w", err)
			}
			expectedHash = strings.TrimSpace(string(hashBytes))

			hashPath := filepath.Join(sliceDir, "sha256.txt")
			if err := os.WriteFile(hashPath, hashBytes, 0644); err != nil {
				os.RemoveAll(sliceDir)
				return nil, fmt.Errorf("failed to write sha256.txt: %w", err)
			}
		}
	}

	if dataLogPath == "" {
		os.RemoveAll(sliceDir)
		return nil, fmt.Errorf("archive missing data.log")
	}
	if expectedHash == "" {
		os.RemoveAll(sliceDir)
		return nil, fmt.Errorf("archive missing sha256.txt")
	}

	actualHash, err := ComputeFileHash(dataLogPath)
	if err != nil {
		os.RemoveAll(sliceDir)
		return nil, fmt.Errorf("failed to compute hash: %w", err)
	}

	if actualHash != expectedHash {
		os.RemoveAll(sliceDir)
		return nil, fmt.Errorf("hash mismatch: expected %s, got %s", expectedHash, actualHash)
	}

	info, err := d.rebuildIndices(sliceDir, sliceNum)
	if err != nil {
		os.RemoveAll(sliceDir)
		return nil, fmt.Errorf("failed to rebuild indices: %w", err)
	}

	return info, nil
}

func (d *SliceDownloader) rebuildIndices(sliceDir string, sliceNum uint32) (*SliceInfo, error) {
	dataLogPath := filepath.Join(sliceDir, "data.log")

	header, err := ReadDataLogHeader(dataLogPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	reader, err := NewReaderWithHeader(dataLogPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open data log: %w", err)
	}
	defer reader.Close()

	blockIndex := NewBlockIndex()

	var lastBlock uint32
	var globMin, globMax uint64
	first := true

	err = reader.ScanWithHeader(func(offset uint64, data []byte) error {
		decompressed, err := decompressData(data)
		if err != nil {
			return fmt.Errorf("failed to decompress block at offset %d: %w", offset, err)
		}

		meta := parseBlockMetadata(decompressed)
		if meta == nil {
			return fmt.Errorf("failed to parse block at offset %d", offset)
		}

		blockIndex.Add(BlockIndexEntry{
			BlockNum: meta.BlockNum,
			Offset:   offset,
			Size:     uint32(len(data)),
			GlobMin:  meta.MinGlobInBlock,
			GlobMax:  meta.MaxGlobInBlock,
		})

		if first {
			globMin = meta.MinGlobInBlock
			first = false
		}
		lastBlock = meta.BlockNum
		globMax = meta.MaxGlobInBlock

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	blockIdxPath := filepath.Join(sliceDir, "blocks.index")
	if err := blockIndex.WriteTo(blockIdxPath); err != nil {
		return nil, fmt.Errorf("failed to write blocks.index: %w", err)
	}

	return &SliceInfo{
		SliceNum:       sliceNum,
		StartBlock:     header.StartBlock,
		EndBlock:       lastBlock,
		MaxBlock:       header.EndBlock,
		BlocksPerSlice: header.BlocksPerSlice,
		Finalized:      header.Finalized == 1,
		GlobMin:        globMin,
		GlobMax:        globMax,
	}, nil
}

func (d *SliceDownloader) applySlice(sliceNum uint32, info *SliceInfo) error {
	return d.store.AddDownloadedSlice(info)
}

func (d *SliceDownloader) downloadAndProcessMega(ctx context.Context, megaNum uint32) sliceResult {
	result := sliceResult{sliceNum: megaNum * SlicesPerMegaDownload}

	archiveName := d.megaArchiveName(megaNum)
	url := d.baseURL + archiveName

	startBlock := megaNum*BlocksPerMegaDownload + 1
	endBlock := (megaNum + 1) * BlocksPerMegaDownload
	logger.Printf("download", "→ Downloading blocks %d-%d...", startBlock, endBlock)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		result.err = fmt.Errorf("failed to create request: %w", err)
		return result
	}

	resp, err := d.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			result.err = fmt.Errorf("download cancelled: %w", ctx.Err())
		} else {
			result.err = fmt.Errorf("download failed: %w", err)
		}
		return result
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		result.notFound = true
		return result
	}

	if resp.StatusCode != http.StatusOK {
		result.err = fmt.Errorf("unexpected status: %d", resp.StatusCode)
		return result
	}

	tmpFile, err := os.CreateTemp("", "mega-*.tar.zst")
	if err != nil {
		result.err = fmt.Errorf("failed to create temp file: %w", err)
		return result
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		tmpFile.Close()
		result.err = fmt.Errorf("download copy failed: %w", err)
		return result
	}
	tmpFile.Close()

	if err := d.extractMegaSlice(tmpPath, megaNum); err != nil {
		result.err = err
		return result
	}

	result.success = true
	result.notFound = false
	return result
}

func (d *SliceDownloader) extractMegaSlice(archivePath string, megaNum uint32) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("failed to open archive: %w", err)
	}
	defer file.Close()

	zr := zstd.NewReader(file)
	defer zr.Close()

	tr := tar.NewReader(zr)

	sliceData := make(map[uint32]map[string][]byte)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar read error: %w", err)
		}

		parts := strings.Split(header.Name, "/")
		if len(parts) != 2 {
			continue
		}

		sliceDir := parts[0]
		filename := parts[1]

		sliceNum := d.parseSliceNum(sliceDir)
		if sliceNum == 0xFFFFFFFF {
			continue
		}

		content, err := io.ReadAll(tr)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", header.Name, err)
		}

		if sliceData[sliceNum] == nil {
			sliceData[sliceNum] = make(map[string][]byte)
		}
		sliceData[sliceNum][filename] = content
	}

	for sliceNum, files := range sliceData {
		if err := d.writeAndRebuildSlice(sliceNum, files); err != nil {
			return fmt.Errorf("failed to write slice %d: %w", sliceNum, err)
		}
	}

	return nil
}

func (d *SliceDownloader) parseSliceNum(sliceDir string) uint32 {
	pattern := `history_(\d{10})-(\d{10})`
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(sliceDir)
	if matches == nil {
		return 0xFFFFFFFF
	}

	startBlock, err := strconv.ParseUint(matches[1], 10, 32)
	if err != nil {
		return 0xFFFFFFFF
	}

	return uint32((startBlock - 1) / uint64(d.blocksPerSlice))
}

func (d *SliceDownloader) writeAndRebuildSlice(sliceNum uint32, files map[string][]byte) error {
	sliceDir := filepath.Join(d.storagePath, d.sliceDirName(sliceNum))

	if err := os.MkdirAll(sliceDir, 0755); err != nil {
		return fmt.Errorf("failed to create slice directory: %w", err)
	}

	dataLogPath := filepath.Join(sliceDir, "data.log")
	if dataLog, ok := files["data.log"]; ok {
		if err := os.WriteFile(dataLogPath, dataLog, 0644); err != nil {
			os.RemoveAll(sliceDir)
			return fmt.Errorf("failed to write data.log: %w", err)
		}
	} else {
		os.RemoveAll(sliceDir)
		return fmt.Errorf("missing data.log")
	}

	hashPath := filepath.Join(sliceDir, "sha256.txt")
	if hashData, ok := files["sha256.txt"]; ok {
		if err := os.WriteFile(hashPath, hashData, 0644); err != nil {
			os.RemoveAll(sliceDir)
			return fmt.Errorf("failed to write sha256.txt: %w", err)
		}

		expectedHash := strings.TrimSpace(string(hashData))
		actualHash, err := ComputeFileHash(dataLogPath)
		if err != nil {
			os.RemoveAll(sliceDir)
			return fmt.Errorf("failed to compute hash: %w", err)
		}

		if actualHash != expectedHash {
			os.RemoveAll(sliceDir)
			return fmt.Errorf("hash mismatch: expected %s, got %s", expectedHash, actualHash)
		}
	} else {
		os.RemoveAll(sliceDir)
		return fmt.Errorf("missing sha256.txt")
	}

	info, err := d.rebuildIndices(sliceDir, sliceNum)
	if err != nil {
		os.RemoveAll(sliceDir)
		return fmt.Errorf("failed to rebuild indices: %w", err)
	}

	if err := d.applySlice(sliceNum, info); err != nil {
		return fmt.Errorf("failed to apply slice: %w", err)
	}

	return nil
}
