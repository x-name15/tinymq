package cluster

import (
	"bufio"
	"crypto/hmac"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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

type PeerSnapshot struct {
	Address  string    `json:"address"`
	Alive    bool      `json:"alive"`
	LastSeen time.Time `json:"last_seen"`
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
	isSynced          bool
	wg                sync.WaitGroup
	quorumSize        int
}

func NewNode(bindAddr string, httpPort string, b *broker.Broker) *Node {
	isDesignatedLeader := os.Getenv("TINYMQ_CLUSTER_LEADER") == "true"
	secret := os.Getenv("TINYMQ_CLUSTER_SECRET")
	if secret == "" {
		if os.Getenv("TINYMQ_CLUSTER_ALLOW_INSECURE") != "true" {
			log.Fatal("[Cluster] FATAL: TINYMQ_CLUSTER_SECRET is not set. Refusing to start an unauthenticated cluster node, since any host reaching the cluster TCP port could inject REPLICATE/BIND_GROUP/HEARTBEAT messages and hijack leadership. Set TINYMQ_CLUSTER_SECRET, or set TINYMQ_CLUSTER_ALLOW_INSECURE=true to explicitly opt into an unauthenticated cluster (testing only).")
		}
		log.Println("[Cluster] WARNING: TINYMQ_CLUSTER_SECRET is not set and TINYMQ_CLUSTER_ALLOW_INSECURE=true. TCP communication is UNAUTHENTICATED. Do not use this outside of local testing.")
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
	b.OnGroupCreate = func(topic string, group string) error {
		return n.ReplicateBinding(topic, group)
	}
	return n
}

func (n *Node) loadPeersFromEnv() {
	nodesEnv := os.Getenv("TINYMQ_CLUSTER_NODES")
	if nodesEnv == "" {
		return
	}
	selfHostname, _ := os.Hostname()
	for _, addr := range strings.Split(nodesEnv, ",") {
		addr = strings.TrimSpace(addr)
		if addr == "" || addr == n.selfAddr() {
			continue
		}
		host, _, _ := net.SplitHostPort(addr)
		if host == selfHostname || strings.HasPrefix(host, selfHostname+".") {
			continue
		}
		n.Peers[addr] = &Peer{Address: addr, IsAlive: false}
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
	n.wg.Add(3)
	go func() { defer n.wg.Done(); n.acceptConnections() }()
	go func() { defer n.wg.Done(); n.gossipLoop() }()
	go func() { defer n.wg.Done(); n.electionTimeoutLoop() }()
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
			if len(parts) < 5 {
				continue
			}
			term := 0
			fmt.Sscanf(parts[1], "%d", &term)
			leaderAddr := parts[2]
			topic := parts[3]
			payloadB64 := parts[4]
			n.mu.Lock()
			trustedLeader := term > n.CurrentTerm || (term == n.CurrentTerm && (n.VotedFor == "" || n.VotedFor == leaderAddr))
			if trustedLeader {
				n.CurrentTerm = term
				n.Role = Follower
				n.VotedFor = leaderAddr
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
				messages := n.broker.GetStateSnapshot()
				for _, msg := range messages {
					payloadB64 := base64.StdEncoding.EncodeToString(msg.Payload)
					body := fmt.Sprintf("REPLICATE %d %s %s %s", term, n.selfAddr(), msg.Topic, payloadB64)
					mac := n.signMessage(body)
					conn.Write([]byte(fmt.Sprintf("%s %s\n", body, mac)))
				}

				syncCompleteBody := "SYNC_COMPLETE"
				syncCompleteMac := n.signMessage(syncCompleteBody)
				conn.Write([]byte(fmt.Sprintf("%s %s\n", syncCompleteBody, syncCompleteMac)))
			}

		case "REQUEST_VOTE":
			if len(parts) < 3 {
				continue
			}
			candidateTerm := 0
			fmt.Sscanf(parts[1], "%d", &candidateTerm)
			candidateAddr := parts[2]
			allowed := n.evaluateVote(candidateTerm, candidateAddr)
			n.mu.RLock()
			currentTerm := n.CurrentTerm
			n.mu.RUnlock()
			if allowed {
				voteBody := fmt.Sprintf("VOTE_GRANTED %d", currentTerm)
				voteMac := n.signMessage(voteBody)
				fmt.Fprintf(conn, "%s %s\n", voteBody, voteMac)
			} else {
				fmt.Fprintf(conn, "VOTE_DENIED %d\n", currentTerm)
			}

		case "BIND_GROUP":
			if len(parts) < 5 {
				continue
			}
			term := 0
			fmt.Sscanf(parts[1], "%d", &term)
			leaderAddr := parts[2]
			topic := parts[3]
			group := parts[4]
			n.mu.Lock()
			trustedLeader := term > n.CurrentTerm || (term == n.CurrentTerm && (n.VotedFor == "" || n.VotedFor == leaderAddr))
			n.mu.Unlock()
			if trustedLeader {
				_, err := n.broker.CreateGroup(topic, group)
				if err == nil {
					log.Printf("[Cluster] Replicated Consumer Group binding: %s -> %s\n", topic, group)
					conn.Write([]byte("BIND_GROUP_ACK\n"))
				} else {
					conn.Write([]byte("BIND_GROUP_ERR\n"))
				}
			} else {
				conn.Write([]byte("BIND_GROUP_DENIED\n"))
			}
		}
	}
}

func (n *Node) calculateQuorum() int {
	n.mu.RLock()
	if n.quorumSize > 0 {
		q := n.quorumSize
		n.mu.RUnlock()
		return q
	}
	n.mu.RUnlock()
	nodesEnv := os.Getenv("TINYMQ_CLUSTER_NODES")
	var q int
	if nodesEnv == "" {
		q = 1
	} else {
		totalClusterSize := len(strings.Split(nodesEnv, ",")) + 1
		q = (totalClusterSize / 2) + 1
	}
	n.mu.Lock()
	n.quorumSize = q
	n.mu.Unlock()
	return q
}

func (n *Node) ReplicateBinding(topic string, group string) error {
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
	timeoutDuration := 500 * time.Millisecond
	if tStr := os.Getenv("TINYMQ_CLUSTER_REPLICATE_TIMEOUT"); tStr != "" {
		if d, err := time.ParseDuration(tStr); err == nil {
			timeoutDuration = d
		}
	}
	body := fmt.Sprintf("BIND_GROUP %d %s %s", term, topic, group)
	mac := n.signMessage(body)
	msg := fmt.Sprintf("%s %s\n", body, mac)
	var successCount atomic.Int32
	successCount.Store(1)
	ackChan := make(chan struct{}, len(peers))
	for _, addr := range peers {
		go func(target string) {
			conn, err := net.DialTimeout("tcp", target, timeoutDuration)
			if err != nil {
				return
			}
			defer conn.Close()
			fmt.Fprint(conn, msg)
			reader := bufio.NewReader(conn)
			conn.SetReadDeadline(time.Now().Add(timeoutDuration))
			resp, _ := reader.ReadString('\n')
			if strings.TrimSpace(resp) == "BIND_GROUP_ACK" {
				successCount.Add(1)
				ackChan <- struct{}{}
			}
		}(addr)
	}
	quorum := n.calculateQuorum()
	timeoutTimer := time.NewTimer(timeoutDuration)
	defer timeoutTimer.Stop()
	for successCount.Load() < int32(quorum) {
		select {
		case <-ackChan:
		case <-timeoutTimer.C:
			return fmt.Errorf("binding quorum timeout: %d/%d ACKs received within %v", successCount.Load(), len(n.Peers)+1, timeoutDuration)
		}
	}
	log.Printf("[Cluster] Consumer Group Binding replicated to %d nodes (Quorum OK)\n", successCount.Load())
	return nil
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
	timeoutDuration := 500 * time.Millisecond
	if tStr := os.Getenv("TINYMQ_CLUSTER_REPLICATE_TIMEOUT"); tStr != "" {
		if d, err := time.ParseDuration(tStr); err == nil {
			timeoutDuration = d
		}
	}
	payloadB64 := base64.StdEncoding.EncodeToString(payload)
	body := fmt.Sprintf("REPLICATE %d %s %s %s", term, n.selfAddr(), topic, payloadB64)
	mac := n.signMessage(body)
	msg := fmt.Sprintf("%s %s\n", body, mac)
	var successCount atomic.Int32
	successCount.Store(1)
	ackChan := make(chan struct{}, len(peers))
	for _, addr := range peers {
		go func(target string) {
			conn, err := net.DialTimeout("tcp", target, timeoutDuration)
			if err != nil {
				return
			}
			defer conn.Close()
			fmt.Fprint(conn, msg)
			reader := bufio.NewReader(conn)
			conn.SetReadDeadline(time.Now().Add(timeoutDuration))
			resp, _ := reader.ReadString('\n')
			if strings.TrimSpace(resp) == "REPLICATE_ACK" {
				successCount.Add(1)
				ackChan <- struct{}{}
			}
		}(addr)
	}
	quorum := n.calculateQuorum()
	timeoutTimer := time.NewTimer(timeoutDuration)
	defer timeoutTimer.Stop()
	for successCount.Load() < int32(quorum) {
		select {
		case <-ackChan:
		case <-timeoutTimer.C:
			return fmt.Errorf("replication quorum timeout: %d/%d ACKs received within %v", successCount.Load(), len(n.Peers)+1, timeoutDuration)
		}
	}
	log.Printf("[Cluster] Message replicated to %d nodes (Quorum OK)\n", successCount.Load())
	return nil
}

func (n *Node) gossipLoop() {
	leaderTicker := time.NewTicker(1 * time.Second)
	followerTicker := time.NewTicker(5 * time.Second)
	defer leaderTicker.Stop()
	defer followerTicker.Stop()
	gossipSem := make(chan struct{}, 10)
	for {
		select {
		case <-leaderTicker.C:
			n.mu.RLock()
			if n.Role == Leader {
				n.dispatchGossip(gossipSem, true)
			}
			n.mu.RUnlock()
		case <-followerTicker.C:
			n.mu.RLock()
			if n.Role != Leader {
				n.dispatchGossip(gossipSem, false)
			}
			n.mu.RUnlock()
		case <-n.quit:
			return
		}
	}
}

func (n *Node) dispatchGossip(sem chan struct{}, isLeader bool) {
	var peersToPing []string
	for addr := range n.Peers {
		peersToPing = append(peersToPing, addr)
	}
	for _, addr := range peersToPing {
		select {
		case sem <- struct{}{}:
			go func(target string) {
				defer func() { <-sem }()
				if isLeader {
					n.sendHeartbeat(target)
				} else {
					n.pingPeer(target)
				}
			}(addr)
		default:
		}
	}
}

func (n *Node) selfAddr() string {
	if self := os.Getenv("TINYMQ_CLUSTER_SELF"); self != "" {
		return self
	}
	return n.Address
}

func (n *Node) pingPeer(target string) {
	conn, err := net.DialTimeout("tcp", target, 2*time.Second)
	if err != nil {
		n.markPeerDead(target)
		return
	}
	defer conn.Close()
	body := fmt.Sprintf("PING %s %s", n.selfAddr(), n.HttpPort)
	mac := n.signMessage(body)
	fmt.Fprintf(conn, "%s %s\n", body, mac)
	reader := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := reader.ReadString('\n')
	if err == nil && strings.TrimSpace(resp) == "PONG" {
		n.markPeerAlive(target)
	} else {
		n.markPeerDead(target)
	}
}

func (n *Node) sendHeartbeat(target string) {
	conn, err := net.DialTimeout("tcp", target, 2*time.Second)
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
	body := fmt.Sprintf("HEARTBEAT %d %s %s", term, n.selfAddr(), advertiseAddr)
	mac := n.signMessage(body)
	fmt.Fprintf(conn, "%s %s\n", body, mac)
	reader := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
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
	if isNewLeader {
		n.isSynced = false
	}
	acceptsHeartbeat := term > n.CurrentTerm || (term == n.CurrentTerm && (n.VotedFor == leader || n.VotedFor == ""))
	if acceptsHeartbeat {
		n.lastHeartbeatSeen = time.Now()
		if n.Role != Follower {
			log.Printf("[Cluster] Stepping down to Follower. Recognized Leader: %s\n", leader)
			n.Role = Follower
		}
		n.CurrentTerm = term
		n.VotedFor = leader
		n.LeaderHttp = leaderHttp
	}
	needsSync := !n.isSynced && n.Role == Follower
	n.mu.Unlock()
	if needsSync {
		n.mu.Lock()
		n.isSynced = true
		n.mu.Unlock()
		go n.requestSync(leader)
	}
}

func (n *Node) requestSync(leaderAddr string) {
	conn, err := net.DialTimeout("tcp", leaderAddr, 5*time.Second)
	if err != nil {
		n.mu.Lock()
		n.isSynced = false
		n.mu.Unlock()
		return
	}
	defer conn.Close()
	log.Printf("[Cluster] Requesting state synchronization from Leader...\n")
	body := fmt.Sprintf("SYNC_REQ %s", n.selfAddr())
	mac := n.signMessage(body)
	fmt.Fprintf(conn, "%s %s\n", body, mac)
	const perMessageDeadline = 30 * time.Second
	conn.SetDeadline(time.Now().Add(perMessageDeadline))
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		conn.SetDeadline(time.Now().Add(perMessageDeadline))
		line = strings.TrimSpace(line)
		parts := strings.Split(line, " ")
		if len(parts) == 0 || parts[0] == "" {
			continue
		}
		receivedMac := parts[len(parts)-1]
		msgBody := strings.Join(parts[:len(parts)-1], " ")
		if !n.verifyMessage(msgBody, receivedMac) {
			log.Printf("[Cluster] SEC-ALERT: Invalid HMAC signature in SYNC response stream, skipping packet safely.")
			continue
		}
		parts = parts[:len(parts)-1]
		cmd := parts[0]
		if cmd == "SYNC_COMPLETE" {
			log.Println("[Cluster] State synchronization complete.")
			return
		}
		if len(parts) >= 5 && cmd == "REPLICATE" {
			topic := parts[3]
			payloadB64 := parts[4]
			if payload, err := base64.StdEncoding.DecodeString(payloadB64); err == nil {
				n.broker.PublishReplicated(topic, payload)
			}
		}
	}
}

func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Role == Leader
}

func (n *Node) GetCurrentTerm() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.CurrentTerm
}

func (n *Node) GetLeaderHTTP() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.Role == Leader {
		advertise := os.Getenv("TINYMQ_CLUSTER_HTTP_ADVERTISE")
		if advertise != "" {
			if !strings.Contains(advertise, ":") {
				return advertise + ":" + n.HttpPort
			}
			return advertise
		}
		return "127.0.0.1:" + n.HttpPort
	}
	if n.LeaderHttp != "" {
		return n.LeaderHttp
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
	for {
		var b [8]byte
		if _, err := cryptorand.Read(b[:]); err != nil {
			log.Printf("[Cluster] WARN: crypto/rand failed, using fixed election timeout: %v", err)
			b[0] = 0x05
		}
		randomOffset := int(binary.LittleEndian.Uint64(b[:]) % 3000)
		timeout := time.Duration(3000+randomOffset) * time.Millisecond
		timer := time.NewTimer(timeout)
		select {
		case <-timer.C:
			n.mu.RLock()
			role := n.Role
			timeoutExpired := time.Since(n.lastHeartbeatSeen) > timeout
			n.mu.RUnlock()
			isDesignatedLeader := os.Getenv("TINYMQ_CLUSTER_LEADER") == "true"
			if role != Leader && timeoutExpired && !isDesignatedLeader {
				n.startElection()
			}
		case <-n.quit:
			timer.Stop()
			return
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.Role = Candidate
	n.CurrentTerm++
	n.VotedFor = n.selfAddr()
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
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()
	body := fmt.Sprintf("REQUEST_VOTE %d %s", term, n.selfAddr())
	mac := n.signMessage(body)
	fmt.Fprintf(conn, "%s %s\n", body, mac)
	reader := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	resp = strings.TrimSpace(resp)
	if strings.HasPrefix(resp, "VOTE_GRANTED") {
		quorum := n.calculateQuorum()
		n.mu.Lock()
		if n.Role == Candidate && n.CurrentTerm == term {
			n.votesReceived++
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
		log.Printf("[Cluster] Rejecting unauthorized peer discovery attempt from: %s\n", addr)
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
	n.wg.Wait()
	log.Println("[Cluster] Node gracefully shut down.")
}

func (n *Node) RoleString() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	switch n.Role {
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	default:
		return "follower"
	}
}

func (n *Node) GetPeersSnapshot() []PeerSnapshot {
	n.mu.RLock()
	defer n.mu.RUnlock()
	peers := make([]PeerSnapshot, 0, len(n.Peers))
	for _, p := range n.Peers {
		peers = append(peers, PeerSnapshot{
			Address:  p.Address,
			Alive:    p.IsAlive,
			LastSeen: p.LastSeen,
		})
	}
	return peers
}
