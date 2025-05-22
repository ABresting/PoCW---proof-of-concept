package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/yourorg/p2p-framework/chrono"
	"github.com/yourorg/p2p-framework/client"
	"github.com/yourorg/p2p-framework/dgraph"
	"github.com/yourorg/p2p-framework/models"
)

func main() {
	// Initialize Dgraph
	dgraph.InitDgraph("localhost:9080")

	// Keys & Node Creation
	privateKey1 := "fad9c8855b740a0b7ed4c221dbad0f33a83a49cad6b3fe8d5817ac83d38b6a19" // N1
	privateKey2 := "a1b2c3d4e5f67890123456789abcdef123456789abcdef123456789abcdef123" // N2
	privateKey3 := "123456789abcdef123456789abcdef123456789abcdef123456789abcdef1234" // N3

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
	fmt.Printf("N1: %s\n", node1.EthAddress)
	fmt.Printf("N2: %s\n", node2.EthAddress)
	fmt.Printf("N3: %s\n", node3.EthAddress)

	// Peer Setup & Mapping
	node1Addr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/8001/p2p/%s", node1.PeerID()))
	node2Addr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/8002/p2p/%s", node2.PeerID()))
	node3Addr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/8003/p2p/%s", node3.PeerID()))
	node1.Peers[node2.EthAddress] = peer.AddrInfo{ID: node2.PeerID(), Addrs: []multiaddr.Multiaddr{node2Addr}}
	node1.Peers[node3.EthAddress] = peer.AddrInfo{ID: node3.PeerID(), Addrs: []multiaddr.Multiaddr{node3Addr}}
	node2.Peers[node1.EthAddress] = peer.AddrInfo{ID: node1.PeerID(), Addrs: []multiaddr.Multiaddr{node1Addr}}
	node2.Peers[node3.EthAddress] = peer.AddrInfo{ID: node3.PeerID(), Addrs: []multiaddr.Multiaddr{node3Addr}}
	node3.Peers[node1.EthAddress] = peer.AddrInfo{ID: node1.PeerID(), Addrs: []multiaddr.Multiaddr{node1Addr}}
	node3.Peers[node2.EthAddress] = peer.AddrInfo{ID: node2.PeerID(), Addrs: []multiaddr.Multiaddr{node2Addr}}
	node1.AddPeerMapping(node2.EthAddress, 2)
	node1.AddPeerMapping(node3.EthAddress, 3)
	node2.AddPeerMapping(node1.EthAddress, 1)
	node2.AddPeerMapping(node3.EthAddress, 3)
	node3.AddPeerMapping(node1.EthAddress, 1)
	node3.AddPeerMapping(node2.EthAddress, 2)

	fmt.Println("Waiting for network...")
	time.Sleep(2 * time.Second)

	// --- Scenarios ---
	fmt.Println("\n---> Step 1: N1 writes k1=v1 (sends to N2)")
	err = node1.Write("k1", "v1", node2.EthAddress)
	if err != nil {
		fmt.Printf("!!! N1 Write k1 failed: %v\n", err)
	}

	// waiting to network level sync
	time.Sleep(1 * time.Second)

	fmt.Println("\n---> Step 2: N1 writes k2=v2 (sends to N2)")
	err = node1.Write("k2", "v2", node2.EthAddress)
	if err != nil {
		fmt.Printf("!!! N1 Write k2 failed: %v\n", err)
	}

	// waiting to network level sync
	time.Sleep(1 * time.Second)

	fmt.Println("\n---> Step 3: N1 writes k3=v3 (sends to N3 - N3 is behind)")
	err = node1.Write("k3", "v3", node3.EthAddress)
	if err != nil {
		fmt.Printf("!!! N1 Write k3 failed: %v\n", err)
	}

	// waiting to network level sync
	time.Sleep(1 * time.Second)

	fmt.Println("\n---> Step 4: N1 writes k4=v4 (sends to N1 - N2 is behind)")
	err = node1.Write("k4", "v4", node2.EthAddress)
	if err != nil {
		fmt.Printf("!!! N1 Write k4 failed: %v\n", err)
	}

	// waiting to network level sync
	time.Sleep(1 * time.Second)

	fmt.Println("\n---> Step 5: N2 writes k5=v5 (sends to N3 - N3 is behind)")
	err = node2.Write("k5", "v5", node3.EthAddress)
	if err != nil {
		fmt.Printf("!!! N2 Write k5 failed: %v\n", err)
	}
	// waiting to network level sync
	time.Sleep(2 * time.Second)

	// --- Final State ---
	fmt.Println("\n\n--- Final States ---")
	printFinalState(1, node1)
	printFinalState(2, node2)
	printFinalState(3, node3)

	// Force commit of any pending events to Dgraph
	fmt.Println("\nCommitting event graphs to Dgraph...")

	// Build and commit CHRONO graph from final node states
	buildChronoGraph([]*client.Client{node1, node2, node3})

	fmt.Println("\n--- Execution Complete ---")
	fmt.Println("Chrono event graph committed to Dgraph.")
	time.Sleep(3 * time.Second)
}

func printFinalState(id int, node *client.Client) {
	// Use exported StateMu which aliases kvStoreMu for read lock safety if needed by GetStore()
	// Or directly use ClockMu and call the GetStore helper which has its own lock.
	node.ClockMu.RLock()
	fmt.Printf("\nNode %d (%s):\n", id, node.EthAddress)
	fmt.Printf("  Clock: %v\n", node.Clock.Values)
	fmt.Printf("  Store: %v\n", node.GetStore()) // GetStore handles its own locking
	node.ClockMu.RUnlock()
}

// buildChronoGraph reconstructs a CHRONO causal event graph from the final states of all nodes
func buildChronoGraph(nodes []*client.Client) {
	graph := chrono.NewEventGraph(0, "reconstructed")

	eventByKey := make(map[string]string)
	nodeEvents := make(map[uint64][]models.EventInfo)
	lastEventPerNode := make(map[uint64]string)

	nodeIDs := make([]uint64, 0, len(nodes))
	nodesByID := make(map[uint64]*client.Client)
	for _, node := range nodes {
		nodeIDs = append(nodeIDs, node.ID)
		nodesByID[node.ID] = node
	}

	for _, node := range nodes {
		nodeStore := node.GetStore()

		events := make([]models.EventInfo, 0, len(nodeStore))
		for k, v := range nodeStore {
			eventName := fmt.Sprintf("%s:%s", k, v)

			keyNum := 0
			if len(k) > 1 {
				fmt.Sscanf(k[1:], "%d", &keyNum)
			}

			events = append(events, models.EventInfo{
				Key:       k,
				Value:     v,
				EventName: eventName,
				KeyNum:    keyNum,
				NodeID:    node.ID,
			})
		}

		sort.Slice(events, func(i, j int) bool {
			return events[i].KeyNum < events[j].KeyNum
		})

		nodeEvents[node.ID] = events
	}

	allEvents := make([]models.EventInfo, 0)
	for _, events := range nodeEvents {
		allEvents = append(allEvents, events...)
	}

	sort.Slice(allEvents, func(i, j int) bool {
		return allEvents[i].KeyNum < allEvents[j].KeyNum
	})

	for _, e := range allEvents {
		if _, exists := eventByKey[e.EventName]; exists {
			continue
		}

		parentIDs := make([]string, 0)

		if e.KeyNum > 1 {
			prevKey := fmt.Sprintf("k%d", e.KeyNum-1)
			for _, v := range nodes {
				store := v.GetStore()
				if prevValue, exists := store[prevKey]; exists {
					prevEventName := fmt.Sprintf("%s:%s", prevKey, prevValue)
					if prevEventID, ok := eventByKey[prevEventName]; ok {
						parentIDs = append(parentIDs, prevEventID)
						break
					}
				}
			}
		}

		if len(parentIDs) == 0 && len(lastEventPerNode) > 0 {
			if lastID, ok := lastEventPerNode[e.NodeID]; ok && lastID != "" {
				parentIDs = append(parentIDs, lastID)
			} else {
				for _, lastID := range lastEventPerNode {
					if lastID != "" {
						parentIDs = append(parentIDs, lastID)
						break
					}
				}
			}
		}

		nodeClock := nodesByID[e.NodeID].Clock.Copy()

		clock := make(map[int]int)
		for _, nID := range nodeIDs {
			if nID == e.NodeID {
				clock[int(nID)] = e.KeyNum
			} else if finalVal, ok := nodeClock.Values[nID]; ok && finalVal > 0 {
				proportion := float64(e.KeyNum) / float64(len(allEvents))
				estimatedVal := int(float64(finalVal) * proportion)
				if estimatedVal > 0 {
					clock[int(nID)] = estimatedVal
				}
			}
		}

		fmt.Printf("Creating event %s with parents: %v\n", e.EventName, parentIDs)
		eventID := graph.AddEvent(e.EventName, e.Key, e.Value, clock, parentIDs)

		eventByKey[e.EventName] = eventID
		lastEventPerNode[e.NodeID] = eventID
	}

	if err := graph.CommitToGraph(); err != nil {
		fmt.Printf("Error committing reconstructed graph: %v\n", err)
	} else {
		fmt.Println("Successfully committed reconstructed CHRONO graph to Dgraph")
	}
}
