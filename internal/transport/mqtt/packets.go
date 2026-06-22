package mqtt

import (
	"encoding/binary"
	"errors"
	"io"
)

// MQTT Control Packet Types
const (
	PacketConnect    = 1
	PacketConnAck    = 2
	PacketPublish    = 3
	PacketPubAck     = 4
	PacketSubscribe  = 8
	PacketSubAck     = 9
	PacketUnsubscribe  = 10
	PacketUnsubAck     = 11
	PacketPingReq    = 12
	PacketPingResp   = 13
	PacketDisconnect = 14
	
)

func readRemainingLength(r io.Reader) (int, error) {
	var multiplier = 1
	var value = 0
	var buf = make([]byte, 1)

	for {
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		encodedByte := buf[0]
		value += int(encodedByte&127) * multiplier
		if multiplier > 128*128*128 {
			return 0, errors.New("malformed remaining length")
		}
		multiplier *= 128
		if (encodedByte & 128) == 0 {
			break
		}
	}
	return value, nil
}

func writeRemainingLength(value int) []byte {
	var buf []byte
	for {
		encodedByte := byte(value % 128)
		value /= 128
		if value > 0 {
			encodedByte |= 128
		}
		buf = append(buf, encodedByte)
		if value <= 0 {
			break
		}
	}
	return buf
}

func readString(buf []byte, offset *int) (string, error) {
	if *offset+2 > len(buf) {
		return "", io.ErrUnexpectedEOF
	}
	length := int(binary.BigEndian.Uint16(buf[*offset : *offset+2]))
	*offset += 2

	if *offset+length > len(buf) {
		return "", io.ErrUnexpectedEOF
	}
	str := string(buf[*offset : *offset+length])
	*offset += length
	return str, nil
}
