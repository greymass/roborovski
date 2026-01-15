package corereader

import (
	"bytes"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWriteBlockChunk(t *testing.T) {
	buf := &bytes.Buffer{}
	blockData := []byte{1, 2, 3, 4, 5}

	err := writeBlockChunkForTest(buf, 12345, blockData)
	if err != nil {
		t.Fatalf("writeBlockChunk failed: %v", err)
	}

	if buf.Len() != 4+4+5 {
		t.Errorf("expected %d bytes, got %d", 4+4+5, buf.Len())
	}

	var blockNum, length uint32
	binary.Read(buf, binary.BigEndian, &blockNum)
	binary.Read(buf, binary.BigEndian, &length)

	if blockNum != 12345 {
		t.Errorf("expected block_num 12345, got %d", blockNum)
	}
	if length != 5 {
		t.Errorf("expected length 5, got %d", length)
	}
}

func writeBlockChunkForTest(w io.Writer, blockNum uint32, data []byte) error {
	if err := binary.Write(w, binary.BigEndian, blockNum); err != nil {
		return err
	}
	dataLen := uint32(len(data))
	if err := binary.Write(w, binary.BigEndian, dataLen); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	return nil
}

func TestParseBlockBatchResponse_SequenceGap(t *testing.T) {
	buf := &bytes.Buffer{}
	writeBlockChunkForTest(buf, 1000, []byte{1, 2, 3})
	writeBlockChunkForTest(buf, 1002, []byte{4, 5, 6})

	hr := &HTTPReader{}

	_, err := hr.parseBlockBatchResponse(buf, 1000, 1002, nil)
	if err == nil {
		t.Fatal("expected error for sequence gap")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("expected 1001, got 1002")) {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestParseBlockBatchResponse_IncompleteBatch(t *testing.T) {
	buf := &bytes.Buffer{}
	writeBlockChunkForTest(buf, 1000, []byte{1, 2, 3})

	hr := &HTTPReader{}

	_, err := hr.parseBlockBatchResponse(buf, 1000, 1010, nil)
	if err == nil {
		t.Fatal("expected error for incomplete batch")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("missing 1001-1010")) {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestParseBlockBatchResponse_EmptyResponse(t *testing.T) {
	buf := &bytes.Buffer{}

	hr := &HTTPReader{}

	_, err := hr.parseBlockBatchResponse(buf, 1000, 1010, nil)
	if err == nil {
		t.Fatal("expected error for empty response")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("no blocks received")) {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestParseBlockBatchResponse_ZeroLength(t *testing.T) {
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, uint32(1000))
	binary.Write(buf, binary.BigEndian, uint32(0))

	hr := &HTTPReader{}

	_, err := hr.parseBlockBatchResponse(buf, 1000, 1000, nil)
	if err == nil {
		t.Fatal("expected error for zero length")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("has zero length")) {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestParseBlockBatchResponse_TooLargeLength(t *testing.T) {
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, uint32(1000))
	binary.Write(buf, binary.BigEndian, uint32(100*1024*1024))

	hr := &HTTPReader{}

	_, err := hr.parseBlockBatchResponse(buf, 1000, 1000, nil)
	if err == nil {
		t.Fatal("expected error for too large length")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("too large")) {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestParseBlockBatchResponse_TruncatedData(t *testing.T) {
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, uint32(1000))
	binary.Write(buf, binary.BigEndian, uint32(100))
	buf.Write([]byte{1, 2, 3})

	hr := &HTTPReader{}

	_, err := hr.parseBlockBatchResponse(buf, 1000, 1000, nil)
	if err == nil {
		t.Fatal("expected error for truncated data")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("incomplete block")) {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestHTTPBatchEndpoint_ErrorResponses(t *testing.T) {
	tests := []struct {
		name       string
		reqBody    string
		wantStatus int
		wantErr    string
	}{
		{
			name:       "invalid start_block",
			reqBody:    `{"start_block":1,"end_block":100}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "start_block must be >= 2",
		},
		{
			name:       "end < start",
			reqBody:    `{"start_block":100,"end_block":50}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "end_block must be >= start_block",
		},
		{
			name:       "batch too large",
			reqBody:    `{"start_block":1000,"end_block":20000}`,
			wantStatus: http.StatusBadRequest,
			wantErr:    "Batch size too large",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/history/get_blocks_batch_binary" {
					t.Errorf("unexpected path: %s", r.URL.Path)
				}

				body, _ := io.ReadAll(r.Body)
				defer r.Body.Close()

				if string(body) == tt.reqBody {
					http.Error(w, tt.wantErr, tt.wantStatus)
				}
			}))
			defer server.Close()

			hr, _ := NewHTTPReader(server.URL, false)
			_, err := hr.GetRawBlockBatchFiltered(1, 100, nil)

			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
