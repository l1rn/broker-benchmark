package generator

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"time"
)

type Message struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Payload   string    `json:"payload"`
	Size      int       `json:"size"`
}

func GenerateMessage(size int) *Message {
	payload := generateRandomString(size)

	return &Message{
		ID:        generateID(),
		Timestamp: time.Now(),
		Payload:   payload,
		Size:      size,
	}
}

func (m *Message) ToJSON([]byte, error) {
	return json.Marshal(m)
}

func generateRandomString(size int) string {
	bytes := make([]byte, size)
	rand.Read(bytes)
	return base64.StdEncoding.EncodeToString(bytes)[:size]
}

func generateID() string {
	b := make([]byte, size)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}

func GenerateBatch(size, count int) [][]byte {
	messages := make([][]byte, count)
	for i := 0; i < count; i++ {
		msg := GenerateMessage(size)
		data, _ := msg.ToJSON()
		messages[i] = data
	}
	return messages
}
