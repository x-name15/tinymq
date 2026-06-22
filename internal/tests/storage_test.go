package tests

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/x-name15/tinymq/internal/message"
	"github.com/x-name15/tinymq/internal/storage"
)

// Validate that messages are correctly written to disk and loaded back into RAM.
func TestStorageWriteAndLoad(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tinymq_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store, err := storage.New(tempDir, false)
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}

	topic := "test.persistence"
	msg := message.Message{
		ID:        "msg-123",
		Topic:     topic,
		Payload:   []byte("hello disk"),
		Timestamp: time.Now(),
	}

	err = store.AppendPut(topic, msg)
	if err != nil {
		t.Fatalf("Failed to write PUT record: %v", err)
	}

	loadedMsgs, err := store.LoadMessages(topic)
	if err != nil {
		t.Fatalf("Failed to load messages: %v", err)
	}

	if len(loadedMsgs) != 1 {
		t.Fatalf("Expected 1 message to be loaded, got %d", len(loadedMsgs))
	}

	if string(loadedMsgs[0].Payload) != "hello disk" {
		t.Errorf("Payload mismatch. Expected 'hello disk', got '%s'", string(loadedMsgs[0].Payload))
	}
}

// Validate that acknowledged messages (ACK) are NOT loaded into RAM upon recovery.
func TestStorageAcks(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "tinymq_test_acks_*")
	defer os.RemoveAll(tempDir)
	store, _ := storage.New(tempDir, false)

	topic := "test.acks"

	store.AppendPut(topic, message.Message{ID: "msg-1", Payload: []byte("keep me")})
	store.AppendPut(topic, message.Message{ID: "msg-2", Payload: []byte("delete me")})

	store.AppendAck(topic, "msg-2")

	loadedMsgs, _ := store.LoadMessages(topic)
	if len(loadedMsgs) != 1 {
		t.Fatalf("Expected 1 message after recovery, got %d", len(loadedMsgs))
	}

	if loadedMsgs[0].ID != "msg-1" {
		t.Errorf("Expected 'msg-1' to survive, but got '%s'", loadedMsgs[0].ID)
	}
}

// Validate that the Garbage Collector correctly compacts the .log file to save disk space.
func TestStorageCompaction(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "tinymq_test_compact_*")
	defer os.RemoveAll(tempDir)
	store, _ := storage.New(tempDir, false)

	topic := "test.compact"

	for i := 0; i < 100; i++ {
		msgID := "msg-" + string(rune(i))
		store.AppendPut(topic, message.Message{ID: msgID})
		store.AppendAck(topic, msgID)
	}

	store.AppendPut(topic, message.Message{ID: "msg-survivor", Payload: []byte("I lived")})

	err := store.CompactLog(topic)
	if err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	loadedMsgs, _ := store.LoadMessages(topic)
	if len(loadedMsgs) != 1 {
		t.Fatalf("Expected exactly 1 survivor message after compaction, got %d", len(loadedMsgs))
	}

	if loadedMsgs[0].ID != "msg-survivor" {
		t.Errorf("Survivor message got corrupted or lost during compaction")
	}
}

// Validate that topics with slashes (MQTT style) are safely flat-mapped to '@' in disk
func TestStorageSlashFlatMapping(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tinymq_test_slashes_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store, err := storage.New(tempDir, false)
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}

	topic := "infra/sensores/temperatura"
	msg := message.Message{
		ID:        "msg-iot-999",
		Topic:     topic,
		Payload:   []byte("flat-mapped content"),
		Timestamp: time.Now(),
	}

	err = store.AppendPut(topic, msg)
	if err != nil {
		t.Fatalf("Storage rejected slash path write: %v", err)
	}

	expectedFile := filepath.Join(tempDir, "infra@sensores@temperatura.log")
	if _, err := os.Stat(expectedFile); os.IsNotExist(err) {
		t.Errorf("Expected flat file '%s' to exist, but it was not found on disk", expectedFile)
	}

	loadedMsgs, err := store.LoadMessages(topic)
	if err != nil {
		t.Fatalf("Failed to load messages with slashes: %v", err)
	}

	if len(loadedMsgs) != 1 || string(loadedMsgs[0].Payload) != "flat-mapped content" {
		t.Errorf("Data corruption or loss during flat-mapped recovery")
	}
}