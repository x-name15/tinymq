package tests

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/x-name15/tinymq/internal/message"
	"github.com/x-name15/tinymq/internal/storage"
)

func TestChecksumWriteAndValidate(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "tinymq_cksum_*")
	defer os.RemoveAll(tempDir)

	store, err := storage.New(tempDir, false)
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}

	topic := "cksum.basic"
	msg := message.Message{
		ID:        "msg-cksum-1",
		Topic:     topic,
		Payload:   []byte(`{"event":"test"}`),
		Timestamp: time.Now(),
	}

	if err := store.AppendPut(topic, msg); err != nil {
		t.Fatalf("AppendPut: %v", err)
	}
	if err := store.CloseAll(); err != nil {
		t.Fatalf("CloseAll: %v", err)
	}

	store2, _ := storage.New(tempDir, false)
	loaded, err := store2.LoadMessages(topic)
	if err != nil {
		t.Fatalf("LoadMessages: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("expected 1 message, got %d", len(loaded))
	}
	if string(loaded[0].Payload) != `{"event":"test"}` {
		t.Errorf("payload mismatch: %s", loaded[0].Payload)
	}
}

func TestChecksumMismatchIsSkipped(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "tinymq_cksum_corrupt_*")
	defer os.RemoveAll(tempDir)

	store, _ := storage.New(tempDir, false)
	topic := "cksum.corrupt"

	good := message.Message{ID: "msg-good", Topic: topic, Payload: []byte("keep")}
	store.AppendPut(topic, good)
	store.CloseAll()

	logPath := filepath.Join(tempDir, "cksum.corrupt.log")
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("cannot read log file: %v", err)
	}

	var rec map[string]interface{}
	if err := json.Unmarshal(raw[:len(raw)-1], &rec); err != nil {
		t.Fatalf("cannot parse record: %v", err)
	}
	rec["checksum"] = float64(0xDEADBEEF)
	corrupted, _ := json.Marshal(rec)
	corrupted = append(corrupted, '\n')

	if err := os.WriteFile(logPath, corrupted, 0644); err != nil {
		t.Fatalf("cannot write corrupted log: %v", err)
	}

	store2, _ := storage.New(tempDir, false)
	loaded, err := store2.LoadMessages(topic)
	if err != nil {
		t.Fatalf("LoadMessages: %v", err)
	}
	if len(loaded) != 0 {
		t.Errorf("expected 0 messages (corrupted record should be skipped), got %d", len(loaded))
	}
}

func TestChecksumBackwardCompatNone(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "tinymq_cksum_compat_*")
	defer os.RemoveAll(tempDir)

	logPath := filepath.Join(tempDir, "cksum.legacy.log")
	legacyMsg := message.Message{
		ID:        "msg-legacy-1",
		Topic:     "cksum.legacy",
		Payload:   []byte("old format"),
		Timestamp: time.Now(),
	}
	type legacyRecord struct {
		Type      string           `json:"type"`
		Message   *message.Message `json:"message,omitempty"`
		Timestamp time.Time        `json:"timestamp"`
	}
	rec := legacyRecord{Type: "PUT", Message: &legacyMsg, Timestamp: time.Now()}
	b, _ := json.Marshal(rec)
	b = append(b, '\n')
	os.WriteFile(logPath, b, 0644)

	store, _ := storage.New(tempDir, false)
	loaded, err := store.LoadMessages("cksum.legacy")
	if err != nil {
		t.Fatalf("LoadMessages: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("expected 1 legacy message, got %d", len(loaded))
	}
	if string(loaded[0].Payload) != "old format" {
		t.Errorf("payload mismatch: %s", loaded[0].Payload)
	}
}

func TestChecksumCompactionRewritesWithChecksum(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "tinymq_cksum_compact_*")
	defer os.RemoveAll(tempDir)

	logPath := filepath.Join(tempDir, "cksum.compact.log")
	topic := "cksum.compact"

	legacyMsg := message.Message{
		ID:        "msg-compact-1",
		Topic:     topic,
		Payload:   []byte("compact me"),
		Timestamp: time.Now(),
	}
	type legacyRecord struct {
		Type      string           `json:"type"`
		Message   *message.Message `json:"message,omitempty"`
		Timestamp time.Time        `json:"timestamp"`
	}
	rec := legacyRecord{Type: "PUT", Message: &legacyMsg, Timestamp: time.Now()}
	b, _ := json.Marshal(rec)
	b = append(b, '\n')
	os.WriteFile(logPath, b, 0644)

	store, _ := storage.New(tempDir, false)
	if err := store.CompactLog(topic); err != nil {
		t.Fatalf("CompactLog: %v", err)
	}

	f, err := os.Open(logPath)
	if err != nil {
		t.Fatalf("cannot open compacted log: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var row map[string]interface{}
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.Fatalf("cannot parse compacted record: %v", err)
		}
		cs, ok := row["checksum"]
		if !ok {
			t.Error("compacted record missing 'checksum' field")
			continue
		}
		if cs.(float64) == 0 {
			t.Error("compacted record has checksum=0, expected a real checksum")
		}
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("error reading compacted log: %v", err)
	}
}

func TestChecksumAckRecordIsAlsoChecksummed(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "tinymq_cksum_ack_*")
	defer os.RemoveAll(tempDir)

	store, _ := storage.New(tempDir, false)
	topic := "cksum.ack"

	store.AppendPut(topic, message.Message{ID: "msg-ack-1", Topic: topic, Payload: []byte("x")})
	store.AppendAck(topic, "msg-ack-1")
	store.CloseAll()

	logPath := filepath.Join(tempDir, "cksum.ack.log")
	f, _ := os.Open(logPath)
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		var row map[string]interface{}
		json.Unmarshal(scanner.Bytes(), &row)
		if cs, ok := row["checksum"]; !ok || cs.(float64) == 0 {
			t.Errorf("line %d: expected non-zero checksum, got %v", lineNum, cs)
		}
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("error reading ack log: %v", err)
	}

	if lineNum != 2 {
		t.Errorf("expected 2 log records (PUT + ACK), got %d", lineNum)
	}

	store2, _ := storage.New(tempDir, false)
	loaded, _ := store2.LoadMessages(topic)
	if len(loaded) != 0 {
		t.Errorf("expected 0 messages after ACK, got %d", len(loaded))
	}
}
