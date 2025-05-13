package client

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/multiformats/go-multiaddr"
	"github.com/yourorg/p2p-framework/message"
)

// ProtocolID is the custom protocol for message exchange
const ProtocolID = "/p2p-framework/1.0.0"

// DiscoveryServiceTag is used for mDNS discovery
const DiscoveryServiceTag = "p2p-framework-mdns"

// Network handles libp2p communication
type Network struct {
	host       host.Host
	ctx        context.Context
	messages   chan *message.Message
	mu         sync.Mutex
	closed     bool
	peers      map[peer.ID]string // Maps peer ID to Ethereum address
	discovered chan peer.AddrInfo
}

// NewNetwork creates a new libp2p network handler
func NewNetwork(listenAddr string) (*Network, error) {
	ctx := context.Background()
	h, err := libp2p.New(
		libp2p.ListenAddrStrings(listenAddr),
		libp2p.EnableNATService(),
		libp2p.EnableHolePunching(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %v", err)
	}

	network := &Network{
		host:       h,
		ctx:        ctx,
		messages:   make(chan *message.Message, 10),
		peers:      make(map[peer.ID]string),
		discovered: make(chan peer.AddrInfo, 10),
	}

	discovery := mdns.NewMdnsService(h, DiscoveryServiceTag, &peerDiscovery{network: network})
	if err := discovery.Start(); err != nil {
		return nil, fmt.Errorf("failed to start mDNS discovery: %v", err)
	}

	h.SetStreamHandler(protocol.ID(ProtocolID), network.handleStream)

	fmt.Printf("Started libp2p host %s with addrs: %v\n", h.ID(), h.Addrs())
	return network, nil
}

// peerDiscovery implements mdns.Notifee
type peerDiscovery struct {
	network *Network
}

// HandlePeerFound sends discovered peers to the channel
func (pd *peerDiscovery) HandlePeerFound(pi peer.AddrInfo) {
	pd.network.discovered <- pi
}

// SendMessage sends a message to a peer
func (n *Network) SendMessage(peerAddr string, msg *message.Message) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		return fmt.Errorf("network is closed")
	}

	addr, err := multiaddr.NewMultiaddr(peerAddr)
	if err != nil {
		return fmt.Errorf("invalid peer address: %v", err)
	}
	pi, err := peer.AddrInfoFromP2pAddr(addr)
	if err != nil {
		return fmt.Errorf("failed to parse peer address: %v", err)
	}

	if err := n.host.Connect(n.ctx, *pi); err != nil {
		return fmt.Errorf("failed to connect to peer %s: %v", pi.ID, err)
	}

	s, err := n.host.NewStream(n.ctx, pi.ID, protocol.ID(ProtocolID))
	if err != nil {
		return fmt.Errorf("failed to open stream to peer %s: %v", pi.ID, err)
	}
	defer s.Close()

	encoder := json.NewEncoder(s)
	if err := encoder.Encode(msg); err != nil {
		return fmt.Errorf("failed to send message to peer %s: %v", pi.ID, err)
	}

	n.peers[pi.ID] = msg.Receiver
	return nil
}

// handleStream processes incoming streams
func (n *Network) handleStream(s network.Stream) {
	defer s.Close()

	decoder := json.NewDecoder(s)
	var msg message.Message
	if err := decoder.Decode(&msg); err != nil {
		fmt.Printf("Failed to decode message: %v\n", err)
		return
	}

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.messages <- &msg
	n.peers[s.Conn().RemotePeer()] = msg.Sender
	n.mu.Unlock()
}

// IncomingMessages returns a channel for receiving messages
func (n *Network) IncomingMessages() <-chan *message.Message {
	return n.messages
}

// DiscoveredPeers returns a channel for discovered peers
func (n *Network) DiscoveredPeers() <-chan peer.AddrInfo {
	return n.discovered
}

// Close shuts down the network
func (n *Network) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		return nil
	}
	n.closed = true

	if err := n.host.Close(); err != nil {
		return fmt.Errorf("failed to close libp2p host: %v", err)
	}

	close(n.messages)
	return nil
}
