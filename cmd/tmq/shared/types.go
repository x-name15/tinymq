package shared

import "time"

const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorCyan   = "\033[36m"
	ColorBold   = "\033[1m"
)

type TopicStat struct {
	Name             string
	MessageCount     int
	WaitingConsumers int
	IsDLQ            bool
	HasWebhooks      bool
}
type StatsResponse struct {
	Stats         []TopicStat `json:"stats"`
	TotalWebhooks int         `json:"total_webhooks"`
}
type CLIMessage struct {
	ID         string    `json:"id"`
	Topic      string    `json:"topic"`
	Payload    []byte    `json:"payload"`
	Timestamp  time.Time `json:"timestamp"`
	RetryCount int       `json:"retry_count"`
}
type ClusterPeer struct {
	Address  string    `json:"address"`
	Alive    bool      `json:"alive"`
	LastSeen time.Time `json:"last_seen"`
}
type ClusterStatusResponse struct {
	ClusteringEnabled bool          `json:"clustering_enabled"`
	Role              string        `json:"role"`
	Term              int           `json:"term"`
	LeaderHTTP        string        `json:"leader_http"`
	Peers             []ClusterPeer `json:"peers"`
}
