package benchmarks

import (
	"os"
	"testing"
	"time"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/cluster"
)

func setupCluster(b *testing.B) (*broker.Broker, *cluster.Node, *cluster.Node) {
	os.Setenv("TINYMQ_CLUSTER_SECRET", "bench-secret")
	os.Setenv("TINYMQ_CLUSTER_LEADER", "true")

	leaderPort := findFreePort(b)
	followerPort := findFreePort(b)
	clusterPort1 := findFreePort(b)
	clusterPort2 := findFreePort(b)

	leaderAddr := "127.0.0.1:" + clusterPort1
	followerAddr := "127.0.0.1:" + clusterPort2
	os.Setenv("TINYMQ_CLUSTER_NODES", followerAddr)

	brkLeader := broker.New(nil)
	brkFollower := broker.New(nil)

	nodeLeader := cluster.NewNode(leaderAddr, leaderPort, brkLeader)
	nodeLeader.Role = cluster.Leader
	nodeLeader.CurrentTerm = 1
	nodeLeader.Peers[followerAddr] = &cluster.Peer{Address: followerAddr, IsAlive: true}

	nodeFollower := cluster.NewNode(followerAddr, followerPort, brkFollower)
	nodeFollower.Role = cluster.Follower
	nodeFollower.CurrentTerm = 1
	nodeFollower.Peers[leaderAddr] = &cluster.Peer{Address: leaderAddr, IsAlive: true}

	if err := nodeLeader.Start(); err != nil {
		b.Fatalf("leader start failed: %v", err)
	}
	if err := nodeFollower.Start(); err != nil {
		b.Fatalf("follower start failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	return brkLeader, nodeLeader, nodeFollower
}

func BenchmarkClusterReplicate(b *testing.B) {
	brkLeader, nodeLeader, nodeFollower := setupCluster(b)
	defer nodeLeader.Stop()
	defer nodeFollower.Stop()

	topic := "bench/cluster"
	payload := []byte("hello cluster")

	brkLeader.CreateTopic(topic, "reject", 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := brkLeader.Publish(topic, payload, nil, "normal", nil, nil, false); err != nil {
			b.Fatalf("publish failed: %v", err)
		}
	}
}

func BenchmarkClusterThroughput(b *testing.B) {
	brkLeader, nodeLeader, nodeFollower := setupCluster(b)
	defer nodeLeader.Stop()
	defer nodeFollower.Stop()

	topic := "bench/cluster"
	payload := []byte("hello cluster")
	brkLeader.CreateTopic(topic, "reject", 0)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := brkLeader.Publish(topic, payload, nil, "normal", nil, nil, false); err != nil {
				b.Fatalf("publish failed: %v", err)
			}
		}
	})
}

func BenchmarkClusterQuorum(b *testing.B) {
	os.Setenv("TINYMQ_CLUSTER_SECRET", "bench-secret")
	os.Setenv("TINYMQ_CLUSTER_LEADER", "true")

	ports := make([]string, 3)
	clusterPorts := make([]string, 3)
	addrs := make([]string, 3)

	for i := 0; i < 3; i++ {
		ports[i] = findFreePort(b)
		clusterPorts[i] = findFreePort(b)
		addrs[i] = "127.0.0.1:" + clusterPorts[i]
	}

	os.Setenv("TINYMQ_CLUSTER_NODES", addrs[1]+","+addrs[2])

	brokers := make([]*broker.Broker, 3)
	nodes := make([]*cluster.Node, 3)

	for i := 0; i < 3; i++ {
		brokers[i] = broker.New(nil)
		nodes[i] = cluster.NewNode(addrs[i], ports[i], brokers[i])
		nodes[i].CurrentTerm = 1
		for j := 0; j < 3; j++ {
			if i != j {
				nodes[i].Peers[addrs[j]] = &cluster.Peer{Address: addrs[j], IsAlive: true}
			}
		}
		if i == 0 {
			nodes[i].Role = cluster.Leader
		} else {
			nodes[i].Role = cluster.Follower
		}
	}

	for i := 0; i < 3; i++ {
		if err := nodes[i].Start(); err != nil {
			b.Fatalf("node %d start failed: %v", i, err)
		}
		defer nodes[i].Stop()
	}
	time.Sleep(100 * time.Millisecond)

	topic := "bench/quorum"
	payload := []byte("hello quorum")
	brokers[0].CreateTopic(topic, "reject", 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := brokers[0].Publish(topic, payload, nil, "normal", nil, nil, false); err != nil {
			b.Fatalf("publish failed: %v", err)
		}
	}
}
