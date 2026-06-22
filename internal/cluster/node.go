package cluster

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
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
	Role              Role
	Peers             map[string]*Peer
	CurrentTerm       int
	VotedFor          string
	votesReceived     int
	mu                sync.RWMutex
	listener          net.Listener
	quit              chan struct{}
	lastHeartbeatSeen time.Time
}

func NewNode(bindAddr string) *Node {
	isDesignatedLeader := os.Getenv("TINYMQ_CLUSTER_LEADER") == "true"

	initialRole := Follower
	if isDesignatedLeader {
		initialRole = Leader
		log.Printf("[Cluster] Designated as LEADER by configuration.")
	}

	n := &Node{
		Address:           bindAddr,
		Role:              initialRole,
		Peers:             make(map[string]*Peer),
		CurrentTerm:       0,
		VotedFor:          "",
		lastHeartbeatSeen: time.Now(),
		quit:              make(chan struct{}),
	}
	n.loadPeersFromEnv()
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
	reader := bufio.NewReader(conn)

	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			return 
		}

		msg = strings.TrimSpace(msg)
		parts := strings.Split(msg, " ")
		cmd := parts[0]

		switch cmd {
		case "PING":
			senderAddr := parts[1]
			n.markPeerAlive(senderAddr)
			conn.Write([]byte("PONG\n"))

		case "HEARTBEAT":
			leaderTerm := 0
			fmt.Sscanf(parts[1], "%d", &leaderTerm)
			leaderAddr := parts[2]

			n.handleHeartbeat(leaderTerm, leaderAddr)
			n.markPeerAlive(leaderAddr)

			conn.Write([]byte("PONG_HEARTBEAT\n"))

		case "REQUEST_VOTE":
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

	fmt.Fprintf(conn, "PING %s\n", n.Address)
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

	fmt.Fprintf(conn, "HEARTBEAT %d %s\n", term, n.Address)

	reader := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	resp, err := reader.ReadString('\n')

	if err == nil && strings.TrimSpace(resp) == "PONG_HEARTBEAT" {
		n.markPeerAlive(target)
	} else {
		n.markPeerDead(target)
	}
}

func (n *Node) handleHeartbeat(term int, leader string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if term >= n.CurrentTerm {
		n.lastHeartbeatSeen = time.Now()
		if n.Role != Follower {
			log.Printf("[Cluster] Stepping down to Follower. Recognized Leader: %s\n", leader)
			n.Role = Follower
		}
		n.CurrentTerm = term
		n.VotedFor = ""
	}
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
			n.mu.Lock()
			role := n.Role
			timeoutExpired := time.Since(n.lastHeartbeatSeen) > 3*time.Second
			isDesignatedLeader := os.Getenv("TINYMQ_CLUSTER_LEADER") == "true"
			n.mu.Unlock()

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

	fmt.Fprintf(conn, "REQUEST_VOTE %d %s\n", term, n.Address)

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
			totalNodes := len(n.Peers) + 1
			quorum := (totalNodes / 2) + 1

			if n.votesReceived >= quorum {
				n.Role = Leader
				log.Printf("[Cluster] Yipiie! We received %d/%d votes. We are the new LEADER for Term %d!\n", n.votesReceived, totalNodes, term)
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