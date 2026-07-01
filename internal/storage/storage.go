package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/x-name15/tinymq/internal/message"
)

const (
	RecordPut = "PUT"
	RecordAck = "ACK"
)

type LogRecord struct {
	Type      string           `json:"type"`
	Message   *message.Message `json:"message,omitempty"`
	MessageID string           `json:"message_id,omitempty"`
	Timestamp time.Time        `json:"timestamp"`
	Checksum  uint32           `json:"checksum,omitempty"`
}

type DiskStorage struct {
	mapMu       sync.Mutex
	dataDir     string
	activeFiles map[string]*os.File
	topicLocks  map[string]*sync.Mutex
	syncWrites  bool
}

func checksumFor(rec LogRecord) uint32 {
	rec.Checksum = 0
	b, err := json.Marshal(rec)
	if err != nil {
		return 0
	}
	return crc32.ChecksumIEEE(b)
}

func validateRecord(rec LogRecord) bool {
	if rec.Checksum == 0 {
		return true
	}
	return checksumFor(rec) == rec.Checksum
}

func New(dataDir string, syncWrites bool) (*DiskStorage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	return &DiskStorage{
		dataDir:     dataDir,
		activeFiles: make(map[string]*os.File),
		topicLocks:  make(map[string]*sync.Mutex),
		syncWrites:  syncWrites,
	}, nil
}

func (s *DiskStorage) lockFor(topic string) *sync.Mutex {
	s.mapMu.Lock()
	defer s.mapMu.Unlock()
	l, ok := s.topicLocks[topic]
	if !ok {
		l = &sync.Mutex{}
		s.topicLocks[topic] = l
	}
	return l
}

func isSafePath(topic string) error {
	if strings.Contains(topic, "..") ||
		strings.Contains(topic, "\\") ||
		strings.HasPrefix(topic, "/") ||
		strings.HasPrefix(topic, "@") {
		return errors.New("unsafe topic name detected, path traversal aborted")
	}
	return nil
}

func topicToFilename(topic string) string {
	safe := strings.ReplaceAll(topic, "/", "_b_")
	safe = strings.ReplaceAll(safe, "@", "_a_")
	return safe
}

func logFilePath(dataDir, topic string) string {
	return filepath.Join(dataDir, fmt.Sprintf("%s.log", topicToFilename(topic)))
}

func (s *DiskStorage) writeRecord(topic string, record LogRecord) error {
	if err := isSafePath(topic); err != nil {
		return err
	}

	record.Checksum = checksumFor(record)
	lock := s.lockFor(topic)
	lock.Lock()
	defer lock.Unlock()
	file, err := s.getOrOpenFile(topic)
	if err != nil {
		return err
	}
	b, err := json.Marshal(record)
	if err != nil {
		return err
	}

	_, err = file.Write(append(b, '\n'))
	if s.syncWrites && err == nil {
		err = file.Sync()
	}

	return err
}

func (s *DiskStorage) getOrOpenFile(topic string) (*os.File, error) {
	s.mapMu.Lock()
	file, exists := s.activeFiles[topic]
	s.mapMu.Unlock()
	if exists {
		return file, nil
	}

	filename := logFilePath(s.dataDir, topic)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	s.mapMu.Lock()
	s.activeFiles[topic] = file
	s.mapMu.Unlock()
	return file, nil
}

func (s *DiskStorage) AppendPut(topic string, msg message.Message) error {
	return s.writeRecord(topic, LogRecord{
		Type:      RecordPut,
		Message:   &msg,
		Timestamp: time.Now(),
	})
}

func (s *DiskStorage) AppendAck(topic string, msgID string) error {
	return s.writeRecord(topic, LogRecord{
		Type:      RecordAck,
		MessageID: msgID,
		Timestamp: time.Now(),
	})
}

func (s *DiskStorage) LoadMessages(topic string) ([]message.Message, error) {
	if err := isSafePath(topic); err != nil {
		return nil, err
	}
	lock := s.lockFor(topic)
	lock.Lock()
	defer lock.Unlock()
	filename := logFilePath(s.dataDir, topic)
	if _, err := os.Stat(filename); os.IsNotExist(err) {

		legacyFilename := filepath.Join(s.dataDir, fmt.Sprintf("%s.log", strings.ReplaceAll(topic, "/", "@")))

		if _, err := os.Stat(legacyFilename); err == nil {
			filename = legacyFilename
		} else {
			return nil, nil
		}
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	msgMap := make(map[string]message.Message)
	var orderedIDs []string
	corruptedCount := 0

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4<<20), 4<<20)

	for scanner.Scan() {
		var rec LogRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			corruptedCount++
			log.Printf("[WAL] Failed to parse record in topic '%s': %v — skipping\n", topic, err)
			continue
		}

		if !validateRecord(rec) {
			corruptedCount++
			log.Printf("[WARN] Checksum mismatch, skipping corrupted record (topic='%s', type=%s)\n", topic, rec.Type)
			continue
		}

		switch rec.Type {
		case RecordPut:
			if rec.Message != nil {
				msgMap[rec.Message.ID] = *rec.Message
				orderedIDs = append(orderedIDs, rec.Message.ID)
			}
		case RecordAck:
			delete(msgMap, rec.MessageID)
		}
	}

	if corruptedCount > 0 {
		log.Printf("[WAL] Topic '%s' recovery: %d corrupted record(s) skipped\n", topic, corruptedCount)
	}

	var activeMessages []message.Message
	for _, id := range orderedIDs {
		if msg, exists := msgMap[id]; exists {
			activeMessages = append(activeMessages, msg)
		}
	}

	return activeMessages, scanner.Err()
}

func (s *DiskStorage) CompactLog(topic string) error {
	if err := isSafePath(topic); err != nil {
		return err
	}
	lock := s.lockFor(topic)
	lock.Lock()
	defer lock.Unlock()
	s.mapMu.Lock()
	if file, exists := s.activeFiles[topic]; exists {
		file.Close()
		delete(s.activeFiles, topic)
	}
	s.mapMu.Unlock()
	filename := logFilePath(s.dataDir, topic)
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil
	}
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	msgMap := make(map[string]LogRecord)
	var orderedIDs []string
	corruptedCount := 0
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4<<20), 4<<20)
	for scanner.Scan() {
		var rec LogRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			corruptedCount++
			log.Printf("[WAL] Compaction: failed to parse record in topic '%s': %v\n", topic, err)
			continue
		}
		if !validateRecord(rec) {
			corruptedCount++
			log.Printf("[WARN] Checksum mismatch, skipping corrupted record (topic='%s', type=%s)\n", topic, rec.Type)
			continue
		}
		switch rec.Type {
		case RecordPut:
			if rec.Message != nil {
				msgMap[rec.Message.ID] = rec
				orderedIDs = append(orderedIDs, rec.Message.ID)
			}
		case RecordAck:
			delete(msgMap, rec.MessageID)
		}
	}
	if err := scanner.Err(); err != nil {
		file.Close()
		return fmt.Errorf("error scanning log file during compaction: %w", err)
	}
	file.Close()
	if corruptedCount > 0 {
		log.Printf("[WAL] Compaction of topic '%s': discarded %d corrupted record(s)\n", topic, corruptedCount)
	}
	tmpFilename := filename + ".tmp"
	tmpFile, err := os.OpenFile(tmpFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	for _, id := range orderedIDs {
		if rec, exists := msgMap[id]; exists {
			rec.Checksum = checksumFor(rec)
			b, err := json.Marshal(rec)
			if err != nil {
				tmpFile.Close()
				os.Remove(tmpFilename)
				return err
			}
			if _, err := tmpFile.Write(append(b, '\n')); err != nil {
				tmpFile.Close()
				os.Remove(tmpFilename)
				return err
			}
		}
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(tmpFilename)
		return fmt.Errorf("failed to sync compacted log: %w", err)
	}
	tmpFile.Close()
	if err := os.Rename(tmpFilename, filename); err != nil {
		return fmt.Errorf("failed to replace old log with compacted version: %w", err)
	}
	if dir, err := os.Open(filepath.Dir(filename)); err == nil {
		dir.Sync()
		dir.Close()
	}
	return nil
}

func (s *DiskStorage) CloseAll() error {
	s.mapMu.Lock()
	defer s.mapMu.Unlock()
	for topic, file := range s.activeFiles {
		file.Sync()
		file.Close()
		delete(s.activeFiles, topic)
	}
	return nil
}

func (s *DiskStorage) ClearLog(topic string) error {
	if err := isSafePath(topic); err != nil {
		return err
	}
	lock := s.lockFor(topic)
	lock.Lock()
	defer lock.Unlock()
	s.mapMu.Lock()
	if file, exists := s.activeFiles[topic]; exists {
		file.Close()
	}
	s.mapMu.Unlock()
	filename := logFilePath(s.dataDir, topic)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err == nil {
		s.mapMu.Lock()
		s.activeFiles[topic] = file
		s.mapMu.Unlock()
	}
	return err
}

func (s *DiskStorage) DeleteLog(topic string) error {
	if err := isSafePath(topic); err != nil {
		return err
	}
	lock := s.lockFor(topic)
	lock.Lock()
	defer lock.Unlock()
	s.mapMu.Lock()
	if file, exists := s.activeFiles[topic]; exists {
		file.Close()
		delete(s.activeFiles, topic)
	}
	s.mapMu.Unlock()
	filename := logFilePath(s.dataDir, topic)
	err := os.Remove(filename)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func FilenameToTopic(filename string) string {
	topic := strings.ReplaceAll(filename, "_b_", "/")
	topic = strings.ReplaceAll(topic, "_a_", "@")
	return topic
}
