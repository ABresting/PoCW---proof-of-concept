package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/yourorg/p2p-framework/client"
	"github.com/yourorg/p2p-framework/dgraph"
)

func main() {
	// Parse command line flags
	singleNodePtr := flag.Int("node", 0, "Specify a single node ID (1-4) to display only that node's graph, or 0 for all nodes")
	flag.Parse()
	singleNode := *singleNodePtr

	// Initialize Dgraph
	dgraph.InitDgraph("localhost:9080")

	// Keys & Node Creation (using 4 nodes)
	privateKey1 := "fad9c8855b740a0b7ed4c221dbad0f33a83a49cad6b3fe8d5817ac83d38b6a19" // N1
	privateKey2 := "a1b2c3d4e5f67890123456789abcdef123456789abcdef123456789abcdef123" // N2
	privateKey3 := "123456789abcdef123456789abcdef123456789abcdef123456789abcdef1234" // N3
	privateKey4 := "4444456789abcdef123456789abcdef123456789abcdef123456789abcdef444" // N4

	node1, err := client.NewClient(1, "/ip4/0.0.0.0/tcp/8001", privateKey1)
	if err != nil {
		panic(err)
	}
	defer node1.Close()
	node2, err := client.NewClient(2, "/ip4/0.0.0.0/tcp/8002", privateKey2)
	if err != nil {
		panic(err)
	}
	defer node2.Close()
	node3, err := client.NewClient(3, "/ip4/0.0.0.0/tcp/8003", privateKey3)
	if err != nil {
		panic(err)
	}
	defer node3.Close()
	node4, err := client.NewClient(4, "/ip4/0.0.0.0/tcp/8004", privateKey4)
	if err != nil {
		panic(err)
	}
	defer node4.Close()

	fmt.Printf("N1: %s\n", node1.EthAddress)
	fmt.Printf("N2: %s\n", node2.EthAddress)
	fmt.Printf("N3: %s\n", node3.EthAddress)
	fmt.Printf("N4: %s\n", node4.EthAddress)

	// Peer Setup & Mapping
	node1Addr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/8001/p2p/%s", node1.PeerID()))
	node2Addr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/8002/p2p/%s", node2.PeerID()))
	node3Addr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/8003/p2p/%s", node3.PeerID()))
	node4Addr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/8004/p2p/%s", node4.PeerID()))

	// Connect all nodes to each other
	node1.Peers[node2.EthAddress] = peer.AddrInfo{ID: node2.PeerID(), Addrs: []multiaddr.Multiaddr{node2Addr}}
	node1.Peers[node3.EthAddress] = peer.AddrInfo{ID: node3.PeerID(), Addrs: []multiaddr.Multiaddr{node3Addr}}
	node1.Peers[node4.EthAddress] = peer.AddrInfo{ID: node4.PeerID(), Addrs: []multiaddr.Multiaddr{node4Addr}}

	node2.Peers[node1.EthAddress] = peer.AddrInfo{ID: node1.PeerID(), Addrs: []multiaddr.Multiaddr{node1Addr}}
	node2.Peers[node3.EthAddress] = peer.AddrInfo{ID: node3.PeerID(), Addrs: []multiaddr.Multiaddr{node3Addr}}
	node2.Peers[node4.EthAddress] = peer.AddrInfo{ID: node4.PeerID(), Addrs: []multiaddr.Multiaddr{node4Addr}}

	node3.Peers[node1.EthAddress] = peer.AddrInfo{ID: node1.PeerID(), Addrs: []multiaddr.Multiaddr{node1Addr}}
	node3.Peers[node2.EthAddress] = peer.AddrInfo{ID: node2.PeerID(), Addrs: []multiaddr.Multiaddr{node2Addr}}
	node3.Peers[node4.EthAddress] = peer.AddrInfo{ID: node4.PeerID(), Addrs: []multiaddr.Multiaddr{node4Addr}}

	node4.Peers[node1.EthAddress] = peer.AddrInfo{ID: node1.PeerID(), Addrs: []multiaddr.Multiaddr{node1Addr}}
	node4.Peers[node2.EthAddress] = peer.AddrInfo{ID: node2.PeerID(), Addrs: []multiaddr.Multiaddr{node2Addr}}
	node4.Peers[node3.EthAddress] = peer.AddrInfo{ID: node3.PeerID(), Addrs: []multiaddr.Multiaddr{node3Addr}}

	// Map Ethereum addresses to VLC IDs
	node1.AddPeerMapping(node2.EthAddress, 2)
	node1.AddPeerMapping(node3.EthAddress, 3)
	node1.AddPeerMapping(node4.EthAddress, 4)

	node2.AddPeerMapping(node1.EthAddress, 1)
	node2.AddPeerMapping(node3.EthAddress, 3)
	node2.AddPeerMapping(node4.EthAddress, 4)

	node3.AddPeerMapping(node1.EthAddress, 1)
	node3.AddPeerMapping(node2.EthAddress, 2)
	node3.AddPeerMapping(node4.EthAddress, 4)

	node4.AddPeerMapping(node1.EthAddress, 1)
	node4.AddPeerMapping(node2.EthAddress, 2)
	node4.AddPeerMapping(node3.EthAddress, 3)

	// Set epoch threshold to 6 to control milestone creation
	// This means we need 6 write messages to trigger each milestone
	node1.SetEpochThreshold(5)
	node2.SetEpochThreshold(5)
	node3.SetEpochThreshold(5)
	node4.SetEpochThreshold(5)

	// If single node mode is specified, check if it's valid
	if singleNode != 0 && (singleNode < 1 || singleNode > 4) {
		fmt.Printf("Invalid node ID %d specified. Must be between 1-4 or 0 for all nodes.\n", singleNode)
		return
	}

	if singleNode != 0 {
		fmt.Printf("Running full experiment but will only visualize Node %d's graph at the end\n", singleNode)
	}

	fmt.Println("Waiting for network...")
	time.Sleep(2 * time.Second)

	fmt.Println("\n=== PHASE 1: Linear events between M0 and M1 ===")

	fmt.Println("\n---> Step 1: N1 writes k1=v1 (sends to N2)")
	err = node1.Write("k1", "v1", node2.EthAddress)
	if err != nil {
		fmt.Printf("!!! N1 Write k1 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 2: N2 writes k2=v2 (sends to N3)")
	err = node2.Write("k2", "v2", node3.EthAddress)
	if err != nil {
		fmt.Printf("!!! N2 Write k2 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 3: N3 writes k3=v3 (sends to N4)")
	err = node3.Write("k3", "v3", node4.EthAddress)
	if err != nil {
		fmt.Printf("!!! N3 Write k3 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 4: N4 writes k4=v4 (sends to N1)")
	err = node4.Write("k4", "v4", node1.EthAddress)
	if err != nil {
		fmt.Printf("!!! N4 Write k4 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 5: N1 writes k5=v5 (sends to N2)")
	err = node1.Write("k5", "v5", node2.EthAddress)
	if err != nil {
		fmt.Printf("!!! N1 Write k5 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 6: N2 writes k6=v6 (sends to N3)")
	err = node2.Write("k6", "v6", node3.EthAddress)
	if err != nil {
		fmt.Printf("!!! N2 Write k6 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 7: N3 writes k7=v7 (sends to N4)")
	err = node3.Write("k7", "v7", node4.EthAddress)
	if err != nil {
		fmt.Printf("!!! N3 Write k7 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 8: N4 writes k8=v8 (sends to N1)")
	err = node4.Write("k8", "v8", node1.EthAddress)
	if err != nil {
		fmt.Printf("!!! N4 Write k8 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 9: N1 writes k9=v9 (sends to N2)")
	err = node1.Write("k9", "v9", node2.EthAddress)
	if err != nil {
		fmt.Printf("!!! N1 Write k9 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 10: N2 writes k10=v10 (sends to N3) - This should trigger M1 creation")
	err = node2.Write("k10", "v10", node3.EthAddress)
	if err != nil {
		fmt.Printf("!!! N2 Write k10 failed: %v\n", err)
	}

	fmt.Println("   Waiting for M1 milestone creation and propagation...")
	time.Sleep(15 * time.Second)

	fmt.Println("\n=== PHASE 2: Events between M1 and M2 ===")

	fmt.Println("\n---> Step 11: N3 writes k11=v11 (sends to N4)")
	err = node3.Write("k11", "v11", node4.EthAddress)
	if err != nil {
		fmt.Printf("!!! N3 Write k11 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 12: N4 writes k12=v12 (sends to N1)")
	err = node4.Write("k12", "v12", node1.EthAddress)
	if err != nil {
		fmt.Printf("!!! N4 Write k12 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 13: N1 writes k13=v13 (sends to N2)")
	err = node1.Write("k13", "v13", node2.EthAddress)
	if err != nil {
		fmt.Printf("!!! N1 Write k13 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 14: N2 writes k14=v14 (sends to N3)")
	err = node2.Write("k14", "v14", node3.EthAddress)
	if err != nil {
		fmt.Printf("!!! N2 Write k14 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 15: N3 writes k15=v15 (sends to N4)")
	err = node3.Write("k15", "v15", node4.EthAddress)
	if err != nil {
		fmt.Printf("!!! N3 Write k15 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 16: N4 writes k16=v16 (sends to N1)")
	err = node4.Write("k16", "v16", node1.EthAddress)
	if err != nil {
		fmt.Printf("!!! N4 Write k16 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 17: N1 writes k17=v17 (sends to N2)")
	err = node1.Write("k17", "v17", node2.EthAddress)
	if err != nil {
		fmt.Printf("!!! N1 Write k17 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 18: N2 writes k18=v18 (sends to N3)")
	err = node2.Write("k18", "v18", node3.EthAddress)
	if err != nil {
		fmt.Printf("!!! N2 Write k18 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 19: N3 writes k19=v19 (sends to N4)")
	err = node3.Write("k19", "v19", node4.EthAddress)
	if err != nil {
		fmt.Printf("!!! N3 Write k19 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 20: N4 writes k20=v20 (sends to N1) - This should trigger M2 creation")
	err = node4.Write("k20", "v20", node1.EthAddress)
	if err != nil {
		fmt.Printf("!!! N4 Write k20 failed: %v\n", err)
	}

	fmt.Println("   Waiting for M2 milestone creation and propagation...")
	time.Sleep(15 * time.Second)

	fmt.Println("\n=== PHASE 3: Events to trigger M:2 milestone ===")

	fmt.Println("\n---> Step 21: N1 writes k21=v21 (sends to N2)")
	err = node1.Write("k21", "v21", node2.EthAddress)
	if err != nil {
		fmt.Printf("!!! N1 Write k21 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 22: N2 writes k22=v22 (sends to N3)")
	err = node2.Write("k22", "v22", node3.EthAddress)
	if err != nil {
		fmt.Printf("!!! N2 Write k22 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 23: N3 writes k23=v23 (sends to N4)")
	err = node3.Write("k23", "v23", node4.EthAddress)
	if err != nil {
		fmt.Printf("!!! N3 Write k23 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 24: N4 writes k24=v24 (sends to N1)")
	err = node4.Write("k24", "v24", node1.EthAddress)
	if err != nil {
		fmt.Printf("!!! N4 Write k24 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 25: N1 writes k25=v25 (sends to N2) - This should trigger M:2 creation")
	err = node1.Write("k25", "v25", node2.EthAddress)
	if err != nil {
		fmt.Printf("!!! N1 Write k25 failed: %v\n", err)
	}

	fmt.Println("   Waiting for M:2 milestone creation and propagation...")
	time.Sleep(15 * time.Second)

	fmt.Println("\n=== PHASE 4: Additional events to ensure M:2 creation ===")

	fmt.Println("\n---> Step 26: N2 writes k26=v26 (sends to N3)")
	err = node2.Write("k26", "v26", node3.EthAddress)
	if err != nil {
		fmt.Printf("!!! N2 Write k26 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 27: N3 writes k27=v27 (sends to N4)")
	err = node3.Write("k27", "v27", node4.EthAddress)
	if err != nil {
		fmt.Printf("!!! N3 Write k27 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 28: N4 writes k28=v28 (sends to N1)")
	err = node4.Write("k28", "v28", node1.EthAddress)
	if err != nil {
		fmt.Printf("!!! N4 Write k28 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 29: N1 writes k29=v29 (sends to N2)")
	err = node1.Write("k29", "v29", node2.EthAddress)
	if err != nil {
		fmt.Printf("!!! N1 Write k29 failed: %v\n", err)
	}
	time.Sleep(3 * time.Second)

	fmt.Println("\n---> Step 30: N2 writes k30=v30 (sends to N3) - This should trigger M:2 creation")
	err = node2.Write("k30", "v30", node3.EthAddress)
	if err != nil {
		fmt.Printf("!!! N2 Write k30 failed: %v\n", err)
	}

	fmt.Println("   Waiting for M:2 milestone creation and propagation...")
	time.Sleep(15 * time.Second)

	fmt.Println("\nWaiting for final synchronization...")
	time.Sleep(8 * time.Second)

	// --- Final State ---
	fmt.Println("\n\n--- Final States ---")
	printFinalState(1, node1)
	printFinalState(2, node2)
	printFinalState(3, node3)
	printFinalState(4, node4)

	// Force commit of any pending events to Dgraph
	fmt.Println("\nCommitting event graphs to Dgraph...")

	// If a specific node is requested, only commit that node's graph
	if singleNode != 0 {
		// Create a map to look up nodes by ID
		nodeMap := map[int]*client.Client{
			1: node1,
			2: node2,
			3: node3,
			4: node4,
		}

		// Get the specified node
		targetNode := nodeMap[singleNode]

		fmt.Printf("Visualizing only Node %d (%s) graph in Dgraph\n", singleNode, targetNode.EthAddress)

		// Commit just that node's graph
		if err := targetNode.EventGraph.CommitToGraph(); err != nil {
			fmt.Printf("Error committing node%d graph: %v\n", singleNode, err)
		} else {
			fmt.Printf("Successfully committed node%d graph to Dgraph\n", singleNode)
		}
	} else {
		// Commit all node graphs
		fmt.Println("Committing all node graphs...")
		for i, node := range []*client.Client{node1, node2, node3, node4} {
			if err := node.EventGraph.CommitToGraph(); err != nil {
				fmt.Printf("Error committing node%d graph: %v\n", i+1, err)
			} else {
				fmt.Printf("Successfully committed node%d graph to Dgraph\n", i+1)
			}
		}
	}

	fmt.Println("\n--- Execution Complete ---")
	fmt.Println("Expected milestones: M0 (genesis), M:1 (after 5 writes), M:2 (after 10 writes)")
	fmt.Println("Chrono event graph committed to Dgraph.")
	fmt.Println("Visit http://localhost:8000 to view the graph in Ratel UI")
	time.Sleep(3 * time.Second)
}

func printFinalState(id int, node *client.Client) {
	node.ClockMu.RLock()
	fmt.Printf("\nNode %d (%s):\n", id, node.EthAddress)
	fmt.Printf("  Clock: %v\n", node.Clock.Values)
	fmt.Printf("  Store: %v\n", node.GetStore())
	node.ClockMu.RUnlock()
}
