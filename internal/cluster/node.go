package cluster

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/x-name15/tinymq/internal/broker"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type Peer struct {
	Address  string
	IsAlive  bool
	LastSeen time.Time
}

type Node struct {
	Address           string
	HttpPort          string
	LeaderHttp        string
	Role              Role
	Peers             map[string]*Peer
	CurrentTerm       int
	VotedFor          string
	votesReceived     int
	mu                sync.RWMutex
	listener          net.Listener
	quit              chan struct{}
	lastHeartbeatSeen time.Time
	broker            *broker.Broker
	clusterSecret     string
}

func NewNode(bindAddr string, httpPort string, b *broker.Broker) *Node {
	isDesignatedLeader := os.Getenv("TINYMQ_CLUSTER_LEADER") == "true"
	secret := os.Getenv("TINYMQ_CLUSTER_SECRET")

	if secret == "" {
		log.Println("[Cluster] WARNING: TINYMQ_CLUSTER_SECRET is not set. TCP communication is unauthenticated!")
	}

	initialRole := Follower
	if isDesignatedLeader {
		initialRole = Leader
		log.Printf("[Cluster] Designated as LEADER by configuration.")
	}

	n := &Node{
		Address:           bindAddr,
		HttpPort:          httpPort,
		Role:              initialRole,
		Peers:             make(map[string]*Peer),
		CurrentTerm:       0,
		VotedFor:          "",
		lastHeartbeatSeen: time.Now(),
		quit:              make(chan struct{}),
		broker:            b,
		clusterSecret:     secret,
	}
	n.loadPeersFromEnv()

	b.OnPublish = func(topic string, payload []byte) error {
		return n.Replicate(topic, payload)
	}
	return n
}

func (n *Node) loadPeersFromEnv() {
	nodesEnv := os.Getenv("TINYMQ_CLUSTER_NODES")
	if nodesEnv == "" {
		return
	}
	addresses := strings.Split(nodesEnv, ",")
	for _, addr := range addresses {
		addr = strings.TrimSpace(addr)
		if addr != "" && addr != n.Address {
			n.Peers[addr] = &Peer{Address: addr, IsAlive: false}
		}
	}
}

func (n *Node) signMessage(message string) string {
	if n.clusterSecret == "" {
		return "NO_MAC"
	}
	mac := hmac.New(sha256.New, []byte(n.clusterSecret))
	mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}

func (n *Node) verifyMessage(message, receivedMac string) bool {
	if n.clusterSecret == "" {
		return true
	}
	expected := n.signMessage(message)
	return hmac.Equal([]byte(expected), []byte(receivedMac))
}

func (n *Node) Start() error {
	l, err := net.Listen("tcp", n.Address)
	if err != nil {
		return fmt.Errorf("cluster failed to bind on %s: %w", n.Address, err)
	}
	n.listener = l
	log.Printf("[Cluster] Node listening for peers on %s\n", n.Address)

	go n.acceptConnections()
	go n.gossipLoop()
	go n.electionTimeoutLoop()
	return nil
}

func (n *Node) acceptConnections() {
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			select {
			case <-n.quit:
				return
			default:
				continue
			}
		}
		go n.handlePeer(conn)
	}
}

func (n *Node) handlePeer(conn net.Conn) {
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(30 * time.Second))
	reader := bufio.NewReader(conn)

	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		conn.SetDeadline(time.Now().Add(30 * time.Second))

		msg = strings.TrimSpace(msg)
		parts := strings.Split(msg, " ")

		if len(parts) == 0 || parts[0] == "" {
			continue
		}

		cmd := parts[0]
		if len(parts) > 1 {
			receivedMac := parts[len(parts)-1]
			msgBody := strings.Join(parts[:len(parts)-1], " ")

			if !n.verifyMessage(msgBody, receivedMac) {
				log.Printf("[Cluster] SEC-ALERT: Token rejection on incoming message. Invalid HMAC signature from client origin: %s", conn.RemoteAddr())
				conn.Write([]byte("ERR_UNAUTHORIZED\n"))
				return
			}
			parts = parts[:len(parts)-1]
		}

		switch cmd {
		case "PING":
			if len(parts) < 3 {
				continue
			}
			senderAddr := parts[1]
			n.markPeerAlive(senderAddr)
			conn.Write([]byte("PONG\n"))

		case "HEARTBEAT":
			if len(parts) < 3 {
				continue
			}
			leaderTerm := 0
			leaderAddr := parts[2]
			leaderHttp := ""
			fmt.Sscanf(parts[1], "%d", &leaderTerm)
			if len(parts) > 3 {
				leaderHttp = parts[3]
			}

			n.handleHeartbeat(leaderTerm, leaderAddr, leaderHttp)
			n.markPeerAlive(leaderAddr)
			conn.Write([]byte("PONG_HEARTBEAT\n"))

		case "REPLICATE":
			if len(parts) < 4 {
				continue
			}
			term := 0
			fmt.Sscanf(parts[1], "%d", &term)
			topic := parts[2]
			payloadB64 := parts[3]

			n.mu.Lock()
			if term >= n.CurrentTerm {
				n.CurrentTerm = term
				n.Role = Follower
				n.lastHeartbeatSeen = time.Now()
				n.mu.Unlock()

				payload, err := base64.StdEncoding.DecodeString(payloadB64)
				if err == nil {
					n.broker.PublishReplicated(topic, payload)
					conn.Write([]byte("REPLICATE_ACK\n"))
				} else {
					conn.Write([]byte("REPLICATE_ERR\n"))
				}
			} else {
				n.mu.Unlock()
				conn.Write([]byte("REPLICATE_DENIED\n"))
			}

		case "SYNC_REQ":
			if len(parts) < 2 {
				continue
			}
			targetAddr := parts[1]

			n.mu.RLock()
			isLeader := n.Role == Leader
			term := n.CurrentTerm
			n.mu.RUnlock()

			if isLeader {
				log.Printf("[Cluster] Sending state snapshot to amnesic node: %s\n", targetAddr)

				snapshot := n.broker.GetStateSnapshot()

				for topic, messages := range snapshot {
					for _, msgData := range messages {
						payloadB64 := base64.StdEncoding.EncodeToString(msgData)
						body := fmt.Sprintf("REPLICATE %d %s %s", term, topic, payloadB64)
						mac := n.signMessage(body)
						syncMsg := fmt.Sprintf("%s %s\n", body, mac)

						conn.Write([]byte(syncMsg))
						reader.ReadString('\n')
					}
				}
			}

		case "REQUEST_VOTE":
			if len(parts) < 3 {
				continue
			}
			candidateTerm := 0
			fmt.Sscanf(parts[1], "%d", &candidateTerm)
			candidateAddr := parts[2]

			allowed := n.evaluateVote(candidateTerm, candidateAddr)
			if allowed {
				fmt.Fprintf(conn, "VOTE_GRANTED %d\n", n.CurrentTerm)
			} else {
				fmt.Fprintf(conn, "VOTE_DENIED %d\n", n.CurrentTerm)
			}
		}
	}
}

func (n *Node) calculateQuorum() int {
	nodesEnv := os.Getenv("TINYMQ_CLUSTER_NODES")
	if nodesEnv == "" {
		return 1
	}
	totalClusterSize := len(strings.Split(nodesEnv, ",")) + 1
	return (totalClusterSize / 2) + 1
}

func (n *Node) Replicate(topic string, payload []byte) error {
	n.mu.RLock()
	role := n.Role
	term := n.CurrentTerm
	var peers []string
	for addr, peer := range n.Peers {
		if peer.IsAlive {
			peers = append(peers, addr)
		}
	}
	n.mu.RUnlock()

	if role != Leader {
		return errors.New("HTTP_PROXY_REQUIRED")
	}

	if len(peers) == 0 {
		return nil
	}

	payloadB64 := base64.StdEncoding.EncodeToString(payload)
	body := fmt.Sprintf("REPLICATE %d %s %s", term, topic, payloadB64)
	mac := n.signMessage(body)
	msg := fmt.Sprintf("%s %s\n", body, mac)

	successCount := 1
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, addr := range peers {
		wg.Add(1)
		go func(target string) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", target, 5*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()

			fmt.Fprint(conn, msg)
			reader := bufio.NewReader(conn)
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			resp, _ := reader.ReadString('\n')
			if strings.TrimSpace(resp) == "REPLICATE_ACK" {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(addr)
	}

	wg.Wait()
	quorum := n.calculateQuorum()
	if successCount >= quorum {
		log.Printf("[Cluster] Message replicated to %d nodes (Quorum OK)\n", successCount)
		return nil
	}
	return fmt.Errorf("replication quorum failed: %d/%d ACKs received", successCount, len(n.Peers)+1)
}

func (n *Node) gossipLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mu.RLock()
			role := n.Role
			var peersToPing []string
			for addr := range n.Peers {
				peersToPing = append(peersToPing, addr)
			}
			n.mu.RUnlock()

			if role == Leader {
				for _, addr := range peersToPing {
					go n.sendHeartbeat(addr)
				}
			} else {
				for _, addr := range peersToPing {
					go n.pingPeer(addr)
				}
			}
		case <-n.quit:
			return
		}
	}
}

func (n *Node) pingPeer(target string) {
	conn, err := net.DialTimeout("tcp", target, 500*time.Millisecond)
	if err != nil {
		n.markPeerDead(target)
		return
	}
	defer conn.Close()

	body := fmt.Sprintf("PING %s %s", n.Address, n.HttpPort)
	mac := n.signMessage(body)
	fmt.Fprintf(conn, "%s %s\n", body, mac)

	reader := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	resp, err := reader.ReadString('\n')

	if err == nil && strings.TrimSpace(resp) == "PONG" {
		n.markPeerAlive(target)
	} else {
		n.markPeerDead(target)
	}
}

func (n *Node) sendHeartbeat(target string) {
	conn, err := net.DialTimeout("tcp", target, 500*time.Millisecond)
	if err != nil {
		n.markPeerDead(target)
		return
	}
	defer conn.Close()

	n.mu.RLock()
	term := n.CurrentTerm
	n.mu.RUnlock()

	advertiseAddr := os.Getenv("TINYMQ_CLUSTER_HTTP_ADVERTISE")
	if advertiseAddr == "" {
		host, _, _ := net.SplitHostPort(n.Address)
		advertiseAddr = host + ":" + n.HttpPort
	} else {
		if !strings.Contains(advertiseAddr, ":") {
			advertiseAddr = advertiseAddr + ":" + n.HttpPort
		}
	}

	body := fmt.Sprintf("HEARTBEAT %d %s %s", term, n.Address, advertiseAddr)
	mac := n.signMessage(body)
	fmt.Fprintf(conn, "%s %s\n", body, mac)

	reader := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	resp, err := reader.ReadString('\n')

	if err == nil && strings.TrimSpace(resp) == "PONG_HEARTBEAT" {
		n.markPeerAlive(target)
	} else {
		n.markPeerDead(target)
	}
}

func (n *Node) handleHeartbeat(term int, leader string, leaderHttp string) {
	n.mu.Lock()

	isNewLeader := n.VotedFor != leader

	if term >= n.CurrentTerm {
		n.lastHeartbeatSeen = time.Now()
		if n.Role != Follower {
			log.Printf("[Cluster] Stepping down to Follower. Recognized Leader: %s\n", leader)
			n.Role = Follower
		}
		n.CurrentTerm = term
		n.VotedFor = leader
		n.LeaderHttp = leaderHttp
	}
	n.mu.Unlock()

	if isNewLeader && n.Role == Follower {
		go n.requestSync(leader)
	}
}

func (n *Node) requestSync(leaderAddr string) {
	conn, err := net.DialTimeout("tcp", leaderAddr, 2*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()
	log.Printf("[Cluster] Requesting state synchronization from Leader...\n")

	body := fmt.Sprintf("SYNC_REQ %s", n.Address)
	mac := n.signMessage(body)
	fmt.Fprintf(conn, "%s %s\n", body, mac)
}

func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Role == Leader
}

func (n *Node) GetLeaderHTTP() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.Role == Leader {
		return "127.0.0.1:" + n.HttpPort
	}
	if n.VotedFor != "" && n.LeaderHttp != "" {
		host, _, _ := net.SplitHostPort(n.VotedFor)
		return host + ":" + n.LeaderHttp
	}
	return ""
}

func (n *Node) evaluateVote(term int, candidate string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if os.Getenv("TINYMQ_CLUSTER_LEADER") == "true" {
		return false
	}
	if term < n.CurrentTerm {
		return false
	}
	if term > n.CurrentTerm {
		n.CurrentTerm = term
		n.Role = Follower
		n.VotedFor = ""
	}
	if n.VotedFor == "" || n.VotedFor == candidate {
		n.VotedFor = candidate
		n.lastHeartbeatSeen = time.Now()
		log.Printf("[Cluster] Vote GRANTED to candidate %s for Term %d\n", candidate, term)
		return true
	}
	return false
}

func (n *Node) electionTimeoutLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mu.RLock()
			role := n.Role
			timeoutExpired := time.Since(n.lastHeartbeatSeen) > 3*time.Second
			n.mu.RUnlock()

			isDesignatedLeader := os.Getenv("TINYMQ_CLUSTER_LEADER") == "true"

			if role != Leader && timeoutExpired && !isDesignatedLeader {
				n.startElection()
			}
		case <-n.quit:
			return
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.Role = Candidate
	n.CurrentTerm++
	n.VotedFor = n.Address
	n.votesReceived = 1
	n.lastHeartbeatSeen = time.Now()
	term := n.CurrentTerm
	log.Printf("[Cluster] Leader timeout! Starting Election for Term %d...\n", term)
	var peersToPing []string
	for addr := range n.Peers {
		peersToPing = append(peersToPing, addr)
	}
	n.mu.Unlock()
	for _, addr := range peersToPing {
		go n.requestVoteFromPeer(addr, term)
	}
}

func (n *Node) requestVoteFromPeer(addr string, term int) {
	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()

	body := fmt.Sprintf("REQUEST_VOTE %d %s", term, n.Address)
	mac := n.signMessage(body)
	fmt.Fprintf(conn, "%s %s\n", body, mac)

	reader := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	resp, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	resp = strings.TrimSpace(resp)
	if strings.HasPrefix(resp, "VOTE_GRANTED") {
		n.mu.Lock()
		if n.Role == Candidate && n.CurrentTerm == term {
			n.votesReceived++
			quorum := n.calculateQuorum()
			if n.votesReceived >= quorum {
				n.Role = Leader
				log.Printf("[Cluster] Yipiie! We received %d votes. We are the new LEADER for Term %d!\n", n.votesReceived, term)
			}
		}
		n.mu.Unlock()
	}
}

func (n *Node) markPeerAlive(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if peer, exists := n.Peers[addr]; exists {
		if !peer.IsAlive {
			log.Printf("[Cluster] Node %s is now ONLINE\n", addr)
		}
		peer.IsAlive = true
		peer.LastSeen = time.Now()
	} else {
		log.Printf("[Cluster] Discovered new node %s\n", addr)
		n.Peers[addr] = &Peer{Address: addr, IsAlive: true, LastSeen: time.Now()}
	}
}

func (n *Node) markPeerDead(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if peer, exists := n.Peers[addr]; exists {
		if peer.IsAlive {
			log.Printf("[Cluster] Node %s went OFFLINE\n", addr)
		}
		peer.IsAlive = false
	}
}

func (n *Node) Stop() {
	close(n.quit)
	if n.listener != nil {
		n.listener.Close()
	}
}
