package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
}

type DiskStorage struct {
	mu          sync.Mutex
	dataDir     string
	activeFiles map[string]*os.File
}

func New(dataDir string) (*DiskStorage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	return &DiskStorage{
		dataDir:     dataDir,
		activeFiles: make(map[string]*os.File),
	}, nil
}

func (s *DiskStorage) writeRecord(topic string, record LogRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, exists := s.activeFiles[topic]
	if !exists {
		filename := filepath.Join(s.dataDir, fmt.Sprintf("%s.log", topic))
		var err error
		file, err = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		s.activeFiles[topic] = file
	}

	bytes, err := json.Marshal(record)
	if err != nil {
		return err
	}

	_, err = file.Write(append(bytes, '\n'))
	return err
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
	s.mu.Lock()
	defer s.mu.Unlock()

	filename := filepath.Join(s.dataDir, fmt.Sprintf("%s.log", topic))
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil, nil
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	msgMap := make(map[string]message.Message)
	var orderedIDs []string

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var rec LogRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
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

	var activeMessages []message.Message
	for _, id := range orderedIDs {
		if msg, exists := msgMap[id]; exists {
			activeMessages = append(activeMessages, msg)
		}
	}

	return activeMessages, scanner.Err()
}

func (s *DiskStorage) CompactLog(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if file, exists := s.activeFiles[topic]; exists {
		file.Close()
		delete(s.activeFiles, topic)
	}

	filename := filepath.Join(s.dataDir, fmt.Sprintf("%s.log", topic))
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil
	}

	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	
	msgMap := make(map[string]LogRecord)
	var orderedIDs []string

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var rec LogRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
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

	tmpFilename := filename + ".tmp"
	tmpFile, err := os.OpenFile(tmpFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	for _, id := range orderedIDs {
		if rec, exists := msgMap[id]; exists {
			bytes, err := json.Marshal(rec)
			if err != nil {
				tmpFile.Close()
				return err
			}
			if _, err := tmpFile.Write(append(bytes, '\n')); err != nil {
				tmpFile.Close()
				return err
			}
		}
	}
	tmpFile.Close()

	if err := os.Rename(tmpFilename, filename); err != nil {
		return fmt.Errorf("failed to replace old log with compacted version: %w", err)
	}

	return nil
}

func (s *DiskStorage) CloseAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for topic, file := range s.activeFiles {
		file.Sync()
		file.Close()
		delete(s.activeFiles, topic)
	}
	return nil
}

func (s *DiskStorage) ClearLog(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if file, exists := s.activeFiles[topic]; exists {
		file.Close()
	}
	
	filename := filepath.Join(s.dataDir, fmt.Sprintf("%s.log", topic))
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err == nil {
		s.activeFiles[topic] = file
	}
	return err
}

func (s *DiskStorage) DeleteLog(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if file, exists := s.activeFiles[topic]; exists {
		file.Close()
		delete(s.activeFiles, topic)
	}
	
	filename := filepath.Join(s.dataDir, fmt.Sprintf("%s.log", topic))
	err := os.Remove(filename)
	if os.IsNotExist(err) {
		return nil 
	}
	return err
}