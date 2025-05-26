package client

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/fnv"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"runtime"
	"strconv"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/yourorg/p2p-framework/dgraph"
	"github.com/yourorg/p2p-framework/message"
	"github.com/yourorg/p2p-framework/vlc"
)

// A p2p Node/client/agent
type Client struct {
	ID          uint64
	Address     string
	PrivateKey  *ecdsa.PrivateKey
	EthAddress  string
	Clock       *vlc.Clock
	ClockMu     sync.RWMutex
	Peers       map[string]peer.AddrInfo
	network     *Network
	kvStore     map[string]string
	kvStoreMu   sync.RWMutex
	messageLog  []*message.Message
	logMu       sync.RWMutex
	StateMu     sync.RWMutex
	replyChan   chan *message.Message
	peerEthToID map[string]uint64
	peerIDToEth map[uint64]string
	hasher      hash.Hash64

	// Node-level event graph
	EventGraph        *dgraph.EventGraph
	eventMu           sync.RWMutex
	lastMessageEvents map[string]string // Maps eventName (k:v) to its event ID
	lastMilestone     string            // ID of the most recent milestone event
	milestoneCreated  bool              // Flag to indicate if a milestone has been created

	// Epoch coordination
	epochMu             sync.RWMutex
	epochNumber         int
	epochMessageCount   int
	epochMessageIDs     []string       // Track message IDs in current epoch
	epochMessageDigests []string       // Track message digests for hashing
	epochThreshold      int            // Configurable threshold for epoch finalization
	lastEpochHash       string         // Hash of the last finalized epoch
	receivedEpochs      map[int]string // Track received epoch finalizations
	sentEpochs          map[int]bool   // Track epochs we've finalized and sent

	// Vote tracking
	votesMu       sync.RWMutex
	receivedVotes map[int]map[string]string // Maps epoch_number -> voter -> signature
}

func NewClient(id uint64, address, privateKeyHex string) (*Client, error) {
	privateKeyHex = strings.TrimPrefix(privateKeyHex, "0x")
	if len(privateKeyHex) != 64 {
		return nil, fmt.Errorf("invalid private key length")
	}
	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %v", err)
	}
	ethAddress := crypto.PubkeyToAddress(privateKey.PublicKey).Hex()
	network, err := NewNetwork(address)
	if err != nil {
		return nil, fmt.Errorf("create network: %v", err)
	}
	client := &Client{
		ID: id, Address: address, PrivateKey: privateKey, EthAddress: ethAddress,
		Clock: vlc.New(), ClockMu: sync.RWMutex{},
		Peers: make(map[string]peer.AddrInfo), network: network,
		kvStore: make(map[string]string), kvStoreMu: sync.RWMutex{},
		messageLog: make([]*message.Message, 0, 100), logMu: sync.RWMutex{},
		StateMu:     sync.RWMutex{},
		replyChan:   make(chan *message.Message, 10),
		peerEthToID: make(map[string]uint64), peerIDToEth: make(map[uint64]string),
		hasher:              fnv.New64a(),
		EventGraph:          dgraph.NewEventGraph(int(id), ethAddress),
		eventMu:             sync.RWMutex{},
		lastMessageEvents:   make(map[string]string),
		lastMilestone:       "",
		milestoneCreated:    false,
		epochMu:             sync.RWMutex{},
		epochNumber:         0,
		epochMessageCount:   0,
		epochMessageIDs:     make([]string, 0),
		epochMessageDigests: make([]string, 0),
		epochThreshold:      10,
		lastEpochHash:       "",
		receivedEpochs:      make(map[int]string),
		sentEpochs:          make(map[int]bool),
		receivedVotes:       make(map[int]map[string]string),
	}
	go client.handleIncomingMessages()
	go client.handleDiscoveredPeers()

	// Start auto-commit for event graph every minute
	client.EventGraph.StartAutoCommit(5 * time.Minute)

	// Create genesis milestone M0
	client.createGenesisMilestone()

	return client, nil
}

// Creates the genesis milestone M0 for the node
func (c *Client) createGenesisMilestone() {
	c.eventMu.Lock()
	defer c.eventMu.Unlock()

	// Define the genesis milestone key and value
	milestoneKey := "M0"
	milestoneValue := fmt.Sprintf("%d", c.ID)

	// Add to local store
	c.kvStoreMu.Lock()
	c.kvStore[milestoneKey] = milestoneValue
	c.kvStoreMu.Unlock()

	// Create empty clock for genesis event
	emptyClock := make(map[int]int)

	// Add to event graph with no parents (it's the first event)
	eventName := fmt.Sprintf("%s:%s", milestoneKey, milestoneValue)
	eventID := c.EventGraph.AddEvent(eventName, milestoneKey, milestoneValue, emptyClock, []string{})

	// Save as milestone
	c.lastMilestone = eventID
	c.milestoneCreated = true

	// Track the latest event
	c.lastMessageEvents[eventName] = eventID

	// Store the latest event ID from this node
	latestFromSenderKey := fmt.Sprintf("latest_from_node_%d", c.ID)
	c.lastMessageEvents[latestFromSenderKey] = eventID

	fmt.Printf("Node %d: Created genesis milestone %s with ID %s\n", c.ID, eventName, eventID)
}

func (c *Client) AddPeerMapping(ethAddr string, vlcID uint64) {
	c.peerEthToID[ethAddr] = vlcID
	c.peerIDToEth[vlcID] = ethAddr
	fmt.Printf("Node %d [G:%d]: ADDED PEER MAPPING: Eth '%s' <-> VLCID %d. Current map: %v\n", c.ID, goid(), ethAddr, vlcID, c.peerEthToID)
}

func (c *Client) PeerID() peer.ID { return c.network.host.ID() }

func (c *Client) Read(key string, peerEthAddr string) (bool, string, error) {
	c.ClockMu.RLock()
	reqClock := c.Clock.Copy()
	c.ClockMu.RUnlock()
	reqID := uuid.New().String()
	request := &message.RequestMessage{Read: &message.ReadRequest{Key: key}}
	msg := message.NewRequestMessage(c.EthAddress, peerEthAddr, reqID, request, reqClock, "")
	return c.sendRequest(peerEthAddr, reqID, msg)
}

func (c *Client) Write(key, value, peerEthAddr string) error {
	fmt.Printf("Node %d [G:%d]: Initiating Write key '%s' value '%s' to %s\n", c.ID, goid(), key, value, peerEthAddr)
	var msgToSend *message.Message
	c.kvStoreMu.Lock()
	c.ClockMu.Lock()
	c.logMu.Lock()
	_, exists := c.kvStore[key]
	if !exists {
		c.kvStore[key] = value
		c.Clock.Inc(c.ID)
		newClock := c.Clock.Copy()
		fmt.Printf("Node %d [G:%d]: Applied key '%s' locally. New Clock: %v\n", c.ID, goid(), key, newClock.Values)
		reqID := uuid.New().String()
		requestPayload := &message.RequestMessage{Write: &message.WriteRequest{Key: key, Value: value}}
		msgToSend = message.NewRequestMessage(c.EthAddress, peerEthAddr, reqID, requestPayload, newClock, "")
		if err := c.signMessage(msgToSend); err != nil {
			c.logMu.Unlock()
			c.ClockMu.Unlock()
			c.kvStoreMu.Unlock()
			return fmt.Errorf("failed sign local write msg %s: %w", key, err)
		}
		c.messageLog = append(c.messageLog, msgToSend)
		fmt.Printf("Node %d [G:%d]: Logged write msg for key '%s'. Log size: %d\n", c.ID, goid(), key, len(c.messageLog))

		// Record event for the local write
		c.recordMessageEvent(msgToSend, "")
	} else {
		fmt.Printf("Node %d [G:%d]: Key '%s' already exists locally. Write aborted (no send).\n", c.ID, goid(), key)
	}
	c.logMu.Unlock()
	c.ClockMu.Unlock()
	c.kvStoreMu.Unlock()
	if msgToSend != nil {
		// Track this message for epoch coordination (outside of locks to avoid deadlock)
		if !strings.HasPrefix(key, "M") { // Don't count milestones toward epoch threshold
			c.trackMessageForEpoch(msgToSend)
		}

		fmt.Printf("Node %d [G:%d]: Sending Write message for key '%s' to %s\n", c.ID, goid(), key, peerEthAddr)
		err := c.sendMessage("", msgToSend)
		if err != nil {
			fmt.Printf("Node %d [G:%d]: ERROR - Failed send write msg key '%s': %v\n", c.ID, goid(), key, err)
			return err
		}
	}
	return nil
}

func (c *Client) Terminate(peerEthAddr string) error {
	reqID := uuid.New().String()
	msg := message.NewTerminateMessage(c.EthAddress, peerEthAddr, reqID, "")
	return c.sendMessage("", msg)
}

func (c *Client) sendMessage(receiverAddr string, msg *message.Message) error {
	if msg.Sender != c.EthAddress {
		return fmt.Errorf("mismatched sender")
	}
	if msg.Signature == "" {
		if err := c.signMessage(msg); err != nil {
			return fmt.Errorf("failed sign msg %s: %w", msg.ReqID, err)
		}
	}
	destAddrStr := receiverAddr
	var destPeerID peer.ID
	if destAddrStr == "" {
		if msg.Receiver == "" {
			return fmt.Errorf("receiver missing")
		}
		peerInfo, exists := c.Peers[msg.Receiver]
		if !exists {
			return fmt.Errorf("unknown peer: %s", msg.Receiver)
		}
		if len(peerInfo.Addrs) == 0 {
			return fmt.Errorf("no addresses for peer: %s", msg.Receiver)
		}
		destPeerID = peerInfo.ID
		baseAddr := peerInfo.Addrs[0].String()
		if !strings.Contains(baseAddr, "/p2p/") {
			destAddrStr = fmt.Sprintf("%s/p2p/%s", baseAddr, destPeerID.String())
		} else {
			destAddrStr = baseAddr
		}
	} else {
		addr, err := multiaddr.NewMultiaddr(destAddrStr)
		if err == nil {
			pidStr, err := addr.ValueForProtocol(multiaddr.P_P2P)
			if err == nil {
				destPeerID, _ = peer.Decode(pidStr)
			}
		}
	}
	clockStr := "nil"
	if msg.MsgClock != nil {
		clockStr = fmt.Sprintf("%v", msg.MsgClock.Values)
	}
	fmt.Printf("Node %d [G:%d] (%s) sending %s to %s (PeerID %s) at %s (ReqID: %s) Clock: %s\n", c.ID, goid(), c.EthAddress, msg.Type, msg.Receiver, destPeerID, destAddrStr, msg.ReqID, clockStr)
	err := c.network.SendMessage(destAddrStr, msg)
	if err != nil {
		return fmt.Errorf("failed send %s to %s: %v", msg.Type, msg.Receiver, err)
	}
	return nil
}

func (c *Client) sendRequest(peerEthAddr, reqID string, msg *message.Message) (bool, string, error) {
	if msg.Request == nil || msg.Request.Read == nil {
		return false, "", fmt.Errorf("sendRequest called with non-read request type or nil request")
	}
	if peerEthAddr == c.EthAddress {
		fmt.Printf("Node %d [G:%d]: Handling self-read request %s locally\n", c.ID, goid(), reqID)
		if err := c.signMessage(msg); err != nil {
			return false, "", fmt.Errorf("sign self-read: %w", err)
		}
		replyMsg := c.handleMessage(msg)
		if replyMsg == nil {
			return false, "", fmt.Errorf("self-read %s no reply generated by handleMessage", reqID)
		}
		if replyMsg.Type == message.TypeReply && replyMsg.Reply != nil {
			if rr := replyMsg.Reply.ReadReply; rr != nil {
				if !rr.CausallyReady {
					return false, "", fmt.Errorf("self-read %s not causally ready", reqID)
				}
				return rr.Found, rr.Value, nil
			} else {
				return false, "", fmt.Errorf("self-read %s received TypeReply but not ReadReply structure", reqID)
			}
		} else {
			return false, "", fmt.Errorf("self-read %s unexpected message type '%s' or nil body returned by handleMessage", reqID, replyMsg.Type)
		}
	}
	if err := c.sendMessage("", msg); err != nil {
		return false, "", fmt.Errorf("send read req %s: %w", reqID, err)
	}
	timeout := time.After(2 * time.Second)
	fmt.Printf("Node %d [G:%d]: Waiting for read reply to %s from %s...\n", c.ID, goid(), reqID, peerEthAddr)
	for {
		select {
		case reply, ok := <-c.replyChan:
			if !ok {
				return false, "", fmt.Errorf("reply chan closed %s", reqID)
			}
			if reply.Type == message.TypeReply && reply.Sender == peerEthAddr && reply.ReqID == reqID {
				if reply.Reply == nil || reply.Reply.ReadReply == nil {
					return false, "", fmt.Errorf("read reply %s nil body", reqID)
				}
				c.ClockMu.Lock()
				c.Clock.Merge([]*vlc.Clock{reply.MsgClock})
				c.Clock.Inc(c.ID)
				localClockAfterReply := c.Clock.Copy()
				c.ClockMu.Unlock()
				fmt.Printf("Node %d [G:%d]: Updated clock post-read reply %s: %v\n", c.ID, goid(), reqID, localClockAfterReply.Values)
				rr := reply.Reply.ReadReply
				if !rr.CausallyReady {
					return false, "", fmt.Errorf("read %s failed: server not ready", reqID)
				}
				return rr.Found, rr.Value, nil
			}
		case <-timeout:
			fmt.Printf("Node %d [G:%d]: Timeout waiting read reply %s from %s\n", c.ID, goid(), reqID, peerEthAddr)
			return false, "", fmt.Errorf("timeout waiting read reply for %s", reqID)
		}
	}
}

func (c *Client) checkCausalConsistency(requestMsgClock *vlc.Clock) (int, bool) {
	if requestMsgClock == nil || len(requestMsgClock.Values) == 0 {
		return vlc.Equal, true
	}
	c.ClockMu.RLock()
	localClock := c.Clock.Copy()
	c.ClockMu.RUnlock()
	comparison := localClock.Compare(requestMsgClock)
	causallyReady := (comparison == vlc.Equal || comparison == vlc.Greater)
	fmt.Printf("Node %d [G:%d]: Causal Check: Local Clock %v vs Request Clock %v -> Comparison=%d, Ready=%t\n", c.ID, goid(), localClock.Values, requestMsgClock.Values, comparison, causallyReady)
	return comparison, causallyReady
}

// checkCanApplyDirectWrite checks conditions for applying a direct write message.
func (c *Client) checkCanApplyDirectWrite(incomingMsg *message.Message) (
	applyAction int, // 0: ignore, 1: apply_as_first, 2: apply_as_plus_one
	requiresSync bool) {

	if incomingMsg == nil || incomingMsg.MsgClock == nil || incomingMsg.Request == nil || incomingMsg.Request.Write == nil {
		return 0, false // Invalid message
	}
	senderEthAddr := incomingMsg.Sender
	remoteClock := incomingMsg.MsgClock
	senderVLCID, knownSender := c.peerEthToID[senderEthAddr]
	if !knownSender {
		fmt.Printf("Node %d [G:%d]: **CRITICAL in checkCanApplyDirectWrite** Unknown sender ID for EthAddr '%s'. Assuming sync needed.\n", c.ID, goid(), senderEthAddr)
		return 0, true // Requires sync because we can't evaluate without sender's VLC ID
	}
	c.ClockMu.RLock()
	localClock := c.Clock.Copy()
	localSenderEntryValue := c.Clock.Values[senderVLCID] // Default 0 if not exists
	c.ClockMu.RUnlock()
	remoteSenderEntryValue := remoteClock.Values[senderVLCID]

	// Rule 1: First meaningful message from this sender?
	if localSenderEntryValue == 0 && remoteSenderEntryValue > 0 {
		// If it's the first message we're seeing from this sender (our clock for them is 0),
		// we accept their state, even if it implies we're behind on other nodes too.
		fmt.Printf("Node %d [G:%d]: Apply rule: FIRST_MESSAGE from sender %d. Remote clock %v. Relaxed check applied.\n", c.ID, goid(), senderVLCID, remoteClock.Values)
		return 1, false // Apply as first message (Action 1)
	}

	isPlusOne := localClock.IsPlusOneIncrement(remoteClock, senderVLCID)
	if isPlusOne {
		fmt.Printf("Node %d [G:%d]: Apply rule: PLUS_ONE increment from sender %d.\n", c.ID, goid(), senderVLCID)
		return 2, false
	}
	comparison := localClock.Compare(remoteClock)
	if comparison == vlc.Less { // We are strictly behind, and it's not a simple +1 increment
		fmt.Printf("Node %d [G:%d]: Apply rule: GAP_DETECTED (local < remote, not +1). Requires Sync.\n", c.ID, goid())
		return 0, true
	}
	// If local is Greater, Equal, or Incomparable (but not strictly Less and not +1)
	fmt.Printf("Node %d [G:%d]: Apply rule: IGNORE/NO_ACTION (local %v vs remote %v, comp %d not meeting other criteria).\n", c.ID, goid(), localClock.Values, remoteClock.Values, comparison)
	return 0, false // No direct apply, no sync forced by this rule.
}

func (c *Client) handleRequest(msg *message.Message) *message.Message {
	if msg.Request == nil || msg.MsgClock == nil {
		fmt.Printf("Node %d [G:%d]: Received invalid request (nil body or clock).\n", c.ID, goid())
		return nil
	}

	if err := c.verifyMessageSignature(msg); err != nil {
		fmt.Printf("Invalid req sig: %v\n", err)
		return nil
	}

	var responseToSend *message.Message = nil

	// --- Handle READ ---
	if msg.Request.Read != nil {
		key := msg.Request.Read.Key
		fmt.Printf("Node %d [G:%d]: Processing Read Req %s key '%s'\n", c.ID, goid(), msg.ReqID, key)
		_, localReadyForReader := c.checkCausalConsistency(msg.MsgClock)
		c.kvStoreMu.RLock()
		value, found := c.kvStore[key]
		c.kvStoreMu.RUnlock()
		c.ClockMu.Lock()
		c.Clock.Merge([]*vlc.Clock{msg.MsgClock})
		c.Clock.Inc(c.ID)
		currentLocalClockForReply := c.Clock.Copy()
		c.ClockMu.Unlock()
		replyBody := &message.ReplyMessage{ReadReply: &message.ReadReply{Key: key, Value: value, Found: found, CausallyReady: localReadyForReader}}
		responseToSend = message.NewReplyMessage(c.EthAddress, msg.Sender, msg.ReqID, replyBody.ReadReply, currentLocalClockForReply, "")
		if !localReadyForReader {
			fmt.Printf("Node %d [G:%d]: Not causally ready for Read Req %s. Reply indicates this. Current local clock after processing read req: %v\n", c.ID, goid(), msg.ReqID, currentLocalClockForReply.Values)
		}
	} else if msg.Request.Write != nil {
		key := msg.Request.Write.Key
		value := msg.Request.Write.Value
		remoteClock := msg.MsgClock

		// Special handling for milestone events - always apply them
		if strings.HasPrefix(key, "M") {
			c.ClockMu.Lock()
			c.kvStoreMu.Lock()

			// Check if we already have this milestone
			if _, exists := c.kvStore[key]; !exists {
				// Apply the milestone and update our clock
				c.kvStore[key] = value
				c.Clock.Merge([]*vlc.Clock{remoteClock})
				c.Clock.Inc(c.ID)

				fmt.Printf("Node %d [G:%d]: APPLYING milestone '%s'='%s' from %s, clock: %v\n",
					c.ID, goid(), key, value, msg.Sender, c.Clock.Values)

				// Create a dummy message to record the event for graph consistency
				dummyMsg := &message.Message{
					Request: &message.RequestMessage{
						Write: &message.WriteRequest{
							Key:   key,
							Value: value,
						},
					},
					MsgClock: c.Clock.Copy(),
					Sender:   msg.Sender,
				}

				// Record the milestone event
				eventID := c.recordMessageEvent(dummyMsg, "")

				// Ensure this milestone is recognized as the latest milestone
				c.eventMu.Lock()
				c.lastMilestone = eventID
				c.milestoneCreated = true
				fmt.Printf("Node %d: Set milestone reference to %s for incoming milestone %s\n",
					c.ID, eventID, key)
				c.eventMu.Unlock()

				// Reset epoch tracking when receiving a milestone from another node
				if msg.Sender != c.EthAddress {
					c.epochMu.Lock()
					// Extract epoch number from milestone key (M:X format)
					var receivedEpochNum int
					if key != "M0" && strings.HasPrefix(key, "M:") {
						fmt.Sscanf(key, "M:%d", &receivedEpochNum)
						if receivedEpochNum > c.epochNumber {
							c.epochNumber = receivedEpochNum
							c.epochMessageCount = 0
							c.epochMessageIDs = make([]string, 0)
							c.epochMessageDigests = make([]string, 0)
							fmt.Printf("Node %d: Reset epoch tracking to %d after receiving milestone %s\n",
								c.ID, receivedEpochNum, key)
						}
					}
					c.epochMu.Unlock()
				}

				// Add a small delay to ensure the milestone is properly processed
				time.Sleep(200 * time.Millisecond)
			} else {
				fmt.Printf("Node %d [G:%d]: Milestone '%s' already exists locally, skipping\n",
					c.ID, goid(), key)
			}

			c.kvStoreMu.Unlock()
			c.ClockMu.Unlock()
		} else {
			// Regular key-value event processing with existing logic
			applyAction, requiresSync := c.checkCanApplyDirectWrite(msg)

			c.ClockMu.Lock()
			if applyAction > 0 {

				c.Clock.Merge([]*vlc.Clock{remoteClock})

				c.Clock.Inc(c.ID)

				currentClockAfterUpdateValues := c.Clock.Copy().Values
				c.kvStoreMu.Lock()
				c.logMu.Lock()
				_, exists := c.kvStore[key]
				if !exists || applyAction == 1 || applyAction == 2 {
					c.kvStore[key] = value
					fmt.Printf("Node %d [G:%d]: APPLYING write key '%s' = '%s'. Action: %d. New Clock: %v\n", c.ID, goid(), key, value, applyAction, currentClockAfterUpdateValues)

					c.recordMessageEvent(msg, "")
				} else {
					fmt.Printf("Node %d [G:%d]: Key '%s' exists. Write from %s with Action %d. Clock updated. New Clock: %v. K/V not changed if not a +1 or first new.\n", c.ID, goid(), key, msg.Sender, applyAction, currentClockAfterUpdateValues)
				}
				foundInLog := false
				for _, logged := range c.messageLog {
					if logged.ReqID == msg.ReqID && logged.Sender == msg.Sender {
						foundInLog = true
						break
					}
				}
				if !foundInLog {
					c.messageLog = append(c.messageLog, msg)
				}
				c.logMu.Unlock()
				c.kvStoreMu.Unlock()

				// Track this message for epoch coordination (outside of locks to avoid deadlock)
				if !strings.HasPrefix(key, "M") && (!exists || applyAction == 1 || applyAction == 2) {
					c.trackMessageForEpoch(msg)
				}
			} else if requiresSync {
				fmt.Printf("Node %d [G:%d]: Write Req %s needs sync. Sending SyncRequest.\n", c.ID, goid(), msg.ReqID)
				localClockCopy := c.Clock.Copy()
				syncReqMsg := message.NewSyncRequestMessage(c.EthAddress, msg.Sender, msg.ReqID, localClockCopy, "")
				responseToSend = syncReqMsg
			} else {
				fmt.Printf("Node %d [G:%d]: Ignoring Write Req %s content (e.g. local clock ahead/concurrent), but merging its clock and incrementing own.\n", c.ID, goid(), msg.ReqID)
				c.Clock.Merge([]*vlc.Clock{remoteClock})
				c.Clock.Inc(c.ID)
			}
			c.ClockMu.Unlock()
		}
	}
	if responseToSend != nil {
		if err := c.signMessage(responseToSend); err != nil {
			fmt.Printf("Failed sign response %s: %v\n", msg.ReqID, err)
			return nil
		}
	}
	return responseToSend
}

func (c *Client) handleSyncRequest(msg *message.Message) *message.Message {
	if msg.SyncRequest == nil || msg.SyncRequest.RequesterClock == nil {
		fmt.Printf("Node %d [G:%d]: Invalid SyncRequest.\n", c.ID, goid())
		return nil
	}
	if err := c.verifyMessageSignature(msg); err != nil {
		fmt.Printf("Node %d [G:%d]: Invalid signature on SyncRequest: %v\n", c.ID, goid(), err)
		return nil
	}
	requesterClock := msg.SyncRequest.RequesterClock
	c.ClockMu.Lock()
	c.Clock.Merge([]*vlc.Clock{requesterClock})
	c.Clock.Inc(c.ID)
	localClockCopyForResponse := c.Clock.Copy()
	c.ClockMu.Unlock()
	c.logMu.RLock()
	localLogSnapshot := make([]*message.Message, len(c.messageLog))
	copy(localLogSnapshot, c.messageLog)
	c.logMu.RUnlock()
	missingMessages := make([]*message.Message, 0)
	if comp := localClockCopyForResponse.Compare(requesterClock); comp == vlc.Greater || comp == vlc.Incomparable {
		for _, logMsg := range localLogSnapshot {
			if logMsg.MsgClock == nil {
				continue
			}
			if logComp := logMsg.MsgClock.Compare(requesterClock); !(logComp == vlc.Less || logComp == vlc.Equal) {
				missingMessages = append(missingMessages, logMsg)
			}
		}
	}
	if len(missingMessages) > 0 {
		sort.SliceStable(missingMessages, func(i, j int) bool {
			return missingMessages[i].MsgClock.Compare(missingMessages[j].MsgClock) == vlc.Less
		})
		syncRespMsg := message.NewSyncResponseMessage(c.EthAddress, msg.Sender, msg.ReqID, missingMessages, localClockCopyForResponse, "")
		if err := c.signMessage(syncRespMsg); err != nil {
			fmt.Printf("Node %d: Failed to sign SyncResponse: %v\n", c.ID, err)
			return nil
		}
		return syncRespMsg
	}
	return nil
}

func (c *Client) handleSyncResponse(msg *message.Message) {
	if msg.SyncResponse == nil || msg.MsgClock == nil {
		fmt.Printf("Node %d [G:%d]: Invalid SyncResponse.\n", c.ID, goid())
		return
	}
	if err := c.verifyMessageSignature(msg); err != nil {
		fmt.Printf("Node %d [G:%d]: Invalid signature on SyncResponse: %v\n", c.ID, goid(), err)
		return
	}
	missingMessages := msg.SyncResponse.MissingMessages
	c.ClockMu.Lock()
	c.Clock.Merge([]*vlc.Clock{msg.MsgClock})
	c.Clock.Inc(c.ID)
	c.ClockMu.Unlock()
	processedCount := 0
	sort.SliceStable(missingMessages, func(i, j int) bool {
		return missingMessages[i].MsgClock.Compare(missingMessages[j].MsgClock) == vlc.Less
	})
	for _, msgToApply := range missingMessages {
		if msgToApply == nil || msgToApply.Request == nil || msgToApply.Request.Write == nil || msgToApply.MsgClock == nil {
			continue
		}
		key := msgToApply.Request.Write.Key
		val := msgToApply.Request.Write.Value
		messageSpecificClock := msgToApply.MsgClock
		c.ClockMu.Lock()
		c.kvStoreMu.Lock()
		c.logMu.Lock()
		localClockBeforeApplySpecificMsg := c.Clock.Copy()
		_, keyExistsLocally := c.kvStore[key]
		comparisonWithMsgSpecificClock := localClockBeforeApplySpecificMsg.Compare(messageSpecificClock)
		appliedThisMessage := false
		if !keyExistsLocally && comparisonWithMsgSpecificClock != vlc.Greater {
			c.kvStore[key] = val
			c.Clock.Merge([]*vlc.Clock{messageSpecificClock})
			c.Clock.Inc(c.ID)
			appliedThisMessage = true
			fmt.Printf("Node %d [G:%d]: APPLYING synced message for new key '%s'.\n", c.ID, goid(), key)

			c.recordMessageEvent(msgToApply, "")
		} else if (keyExistsLocally || !keyExistsLocally) && (comparisonWithMsgSpecificClock == vlc.Less || comparisonWithMsgSpecificClock == vlc.Incomparable) {
			c.Clock.Merge([]*vlc.Clock{messageSpecificClock})
			c.Clock.Inc(c.ID)
			appliedThisMessage = true
			// fmt.Printf("Node %d [G:%d]: Synced msg key '%s', K/V not applied or already exists, but CLOCK updated due to new info (Local %v vs Msg %v -> Comp %d).\n", c.ID, goid(), key, localClockBeforeApplySpecificMsg.Values, messageSpecificClock.Values, comparisonWithMsgSpecificClock) // Can be verbose
		} else {
			// fmt.Printf("Node %d [G:%d]: Skipping synced msg key '%s'. KeyExists: %t, LocalClockVsMsgSpecificClock: %d.\n", c.ID, goid(), key, keyExistsLocally, comparisonWithMsgSpecificClock) // Can be verbose
		}

		if appliedThisMessage {
			foundInLog := false
			for _, loggedMsg := range c.messageLog {
				if loggedMsg.ReqID == msgToApply.ReqID && loggedMsg.Sender == msgToApply.Sender {
					foundInLog = true
					break
				}
			}
			if !foundInLog {
				c.messageLog = append(c.messageLog, msgToApply)
				processedCount++
			}
		}
		c.logMu.Unlock()
		c.kvStoreMu.Unlock()
		c.ClockMu.Unlock()
	}
	c.logMu.Lock()
	if len(c.messageLog) > 1 {
		sort.SliceStable(c.messageLog, func(i, j int) bool {
			return c.messageLog[i].MsgClock.Compare(c.messageLog[j].MsgClock) == vlc.Less
		})
	}
	c.logMu.Unlock()

	fmt.Printf("Node %d: Finished SyncResponse. Processed %d updates.\n", c.ID, processedCount)
}

func (c *Client) handleMessage(msg *message.Message) *message.Message {
	if msg == nil {
		return nil
	}

	var response *message.Message
	switch msg.Type {
	case message.TypeP2P:
		if msg.P2P != nil && msg.MsgClock != nil {
			if err := c.verifyMessageSignature(msg); err == nil {
				// Don't update clocks for consensus messages
				if msg.P2P.Data != "" && strings.Contains(msg.P2P.Data, "epoch_finalize") {
					c.handleEpochFinalization(msg)
				} else if msg.P2P.Data != "" && strings.Contains(msg.P2P.Data, "epoch_vote") {
					c.handleEpochVote(msg)
				} else {
					c.ClockMu.Lock()
					c.Clock.Merge([]*vlc.Clock{msg.MsgClock})
					c.Clock.Inc(c.ID)
					c.ClockMu.Unlock()
					fmt.Printf("Node %d: Unhandled P2P message type: %s\n", c.ID, msg.P2P.Data)
				}
			} else {
				fmt.Printf("Invalid P2P sig: %v\n", err)
			}
		}
	case message.TypeRequest:
		response = c.handleRequest(msg)
	case message.TypeSyncRequest:
		response = c.handleSyncRequest(msg)
	case message.TypeSyncResponse:
		c.handleSyncResponse(msg)
	case message.TypeReply:
		if err := c.verifyMessageSignature(msg); err == nil {
			select {
			case c.replyChan <- msg:
			default:
				fmt.Printf("WARN: Reply chan full for ReqID %s\n", msg.ReqID)
			}
		} else {
			fmt.Printf("Invalid Reply sig: %v\n", err)
		}
	case message.TypeTerminate:
		fmt.Printf("Node %d [G:%d]: Received Terminate from %s\n", c.ID, goid(), msg.Sender)
	default:
		fmt.Printf("Node %d [G:%d]: Unknown message type '%s'\n", c.ID, goid(), msg.Type)
	}
	return response
}

// handleEpochFinalization processes an incoming epoch finalization message
func (c *Client) handleEpochFinalization(msg *message.Message) {
	if msg == nil || msg.P2P == nil {
		return
	}

	content := msg.P2P.Data

	if content == "" {
		return
	}

	var epochNumber int
	var epochHash string
	var sender string
	var messageCount int

	if numField := strings.Index(content, "epoch_number:"); numField > 0 {
		numStr := content[numField+13:]
		numEnd := strings.Index(numStr, " ")
		if numEnd > 0 {
			epochNumber, _ = strconv.Atoi(numStr[:numEnd])
		}
	}

	if hashField := strings.Index(content, "epoch_hash:"); hashField > 0 {
		hashStr := content[hashField+11:]
		hashEnd := strings.Index(hashStr, " ")
		if hashEnd > 0 {
			epochHash = hashStr[:hashEnd]
		}
	}

	if senderField := strings.Index(content, "sender:"); senderField > 0 {
		senderStr := content[senderField+7:]
		senderEnd := strings.Index(senderStr, " ")
		if senderEnd > 0 {
			sender = senderStr[:senderEnd]
		}
	}

	if countField := strings.Index(content, "message_count:"); countField > 0 {
		countStr := content[countField+14:]
		countEnd := strings.Index(countStr, " ")
		if countEnd > 0 {
			messageCount, _ = strconv.Atoi(countStr[:countEnd])
		} else {
			countEnd = strings.Index(countStr, "]")
			if countEnd > 0 {
				messageCount, _ = strconv.Atoi(countStr[:countEnd])
			}
		}
	}

	if epochNumber == 0 || epochHash == "" {
		fmt.Printf("Node %d: Received malformed epoch finalization: %s\n", c.ID, content)
		return
	}

	c.epochMu.Lock()
	defer c.epochMu.Unlock()

	if _, exists := c.receivedEpochs[epochNumber]; !exists {
		c.receivedEpochs[epochNumber] = epochHash

		if epochNumber > c.epochNumber {
			c.epochNumber = epochNumber
		}

		fmt.Printf("Node %d: RECEIVED EPOCH %d from %s with %d messages, hash: %s\n",
			c.ID, epochNumber, sender, messageCount, epochHash)

		if sender != "" && sender != c.EthAddress {
			go c.sendEpochVote(epochNumber, epochHash, sender)
		}
	}
}

// sendEpochVote creates and sends a vote for an epoch finalization
func (c *Client) sendEpochVote(epochNumber int, epochHash string, recipient string) {
	hashBytes := crypto.Keccak256([]byte(epochHash))
	signature, err := crypto.Sign(hashBytes, c.PrivateKey)
	if err != nil {
		fmt.Printf("Node %d: Failed to sign epoch vote: %v\n", c.ID, err)
		return
	}
	signatureHex := hexutil.Encode(signature)

	voteMsg := map[string]interface{}{
		"type":         "epoch_vote",
		"epoch_number": epochNumber,
		"signature":    signatureHex,
		"voter":        c.EthAddress,
	}
	voteMsgStr := fmt.Sprintf("%v", voteMsg)
	reqID := uuid.New().String()

	// For consensus messages, use an empty vector clock instead of the current clock
	// This way they don't affect causal ordering
	emptyClockForConsensus := vlc.New()
	msg := message.NewP2PMessage(c.EthAddress, recipient, reqID, voteMsgStr, "", emptyClockForConsensus)

	// Sign and send the vote
	if err := c.signMessage(msg); err != nil {
		fmt.Printf("Node %d: Failed to sign vote message: %v\n", c.ID, err)
		return
	}

	if err := c.sendMessage("", msg); err != nil {
		fmt.Printf("Node %d: Failed to send vote for epoch %d to %s: %v\n",
			c.ID, epochNumber, recipient, err)
	} else {
		fmt.Printf("Node %d: Sent VOTE for epoch %d to %s\n", c.ID, epochNumber, recipient)
	}
}

// handleEpochVote processes a received vote for an epoch
func (c *Client) handleEpochVote(msg *message.Message) {
	if msg == nil || msg.P2P == nil || msg.P2P.Data == "" {
		return
	}

	content := msg.P2P.Data
	var epochNumber int
	var signature string
	var voter string

	extractField := func(fieldName string, content string) string {
		patterns := []string{
			fmt.Sprintf("%s:", fieldName),
			fmt.Sprintf("%s ", fieldName),
			fmt.Sprintf("[\"%s\"]:", fieldName),
			fmt.Sprintf("[%s]:", fieldName),
			fmt.Sprintf("map[%s:", fieldName),
			fmt.Sprintf("map[\"%s\":", fieldName),
		}

		for _, pattern := range patterns {
			if idx := strings.Index(content, pattern); idx >= 0 {
				afterPattern := content[idx+len(pattern):]
				valueEnd := -1
				for _, possibleEnd := range []string{" ", ",", "}", "]", "map"} {
					if end := strings.Index(afterPattern, possibleEnd); end > 0 {
						if valueEnd == -1 || end < valueEnd {
							valueEnd = end
						}
					}
				}

				if valueEnd > 0 {
					value := strings.TrimSpace(afterPattern[:valueEnd])
					return strings.Trim(value, "\"'")
				} else {
					return strings.TrimSpace(afterPattern)
				}
			}
		}
		return ""
	}

	epochStr := extractField("epoch_number", content)
	signature = extractField("signature", content)
	voter = extractField("voter", content)

	if epochStr != "" {
		epochNumber, _ = strconv.Atoi(epochStr)
	}

	if epochNumber == 0 || signature == "" || voter == "" {
		fmt.Printf("Node %d: Received malformed epoch vote\n", c.ID)
		return
	}

	c.epochMu.RLock()
	epochHash, exists := c.receivedEpochs[epochNumber]
	wasSentByUs := c.sentEpochs[epochNumber]
	c.epochMu.RUnlock()

	if !exists {
		fmt.Printf("Node %d: Received vote for unknown epoch %d from %s\n", c.ID, epochNumber, voter)
		return
	}

	signatureBytes, err := hexutil.Decode(signature)
	if err != nil {
		fmt.Printf("Node %d: Invalid signature format in vote\n", c.ID)
		return
	}

	messageHash := crypto.Keccak256([]byte(epochHash))
	pubKey, err := crypto.SigToPub(messageHash, signatureBytes)
	if err != nil {
		fmt.Printf("Node %d: Failed to recover public key from signature\n", c.ID)
		return
	}

	recoveredAddress := crypto.PubkeyToAddress(*pubKey).Hex()
	if !strings.EqualFold(recoveredAddress, voter) {
		fmt.Printf("Node %d: Signature verification failed\n", c.ID)
		return
	}

	fmt.Printf("Node %d: RECEIVED VALID VOTE for epoch %d from %s\n", c.ID, epochNumber, voter)

	if wasSentByUs {
		c.votesMu.Lock()

		if _, ok := c.receivedVotes[epochNumber]; !ok {
			c.receivedVotes[epochNumber] = make(map[string]string)
		}

		c.receivedVotes[epochNumber][voter] = signature

		allVoted := true
		for peerAddr := range c.Peers {
			if peerAddr == c.EthAddress {
				continue // Skip self
			}

			if _, hasVoted := c.receivedVotes[epochNumber][peerAddr]; !hasVoted {
				allVoted = false
				break
			}
		}

		c.votesMu.Unlock()

		if allVoted {
			fmt.Printf("Node %d: Received all votes for epoch %d - Creating milestone\n", c.ID, epochNumber)

			milestoneKey := fmt.Sprintf("M:%d", epochNumber)
			milestoneValue := fmt.Sprintf("%d", c.ID)

			c.kvStoreMu.Lock()
			c.ClockMu.Lock()

			if _, exists := c.kvStore[milestoneKey]; !exists {
				c.kvStore[milestoneKey] = milestoneValue
				c.Clock.Inc(c.ID)

				fmt.Printf("Node %d: Created local milestone %s:%s\n",
					c.ID, milestoneKey, milestoneValue)

				dummyMsg := &message.Message{
					Request: &message.RequestMessage{
						Write: &message.WriteRequest{
							Key:   milestoneKey,
							Value: milestoneValue,
						},
					},
					MsgClock: c.Clock.Copy(),
					Sender:   c.EthAddress,
				}

				c.recordMessageEvent(dummyMsg, "")

				// Add a small delay to ensure the milestone is properly recorded
				// before any subsequent messages are processed
				time.Sleep(200 * time.Millisecond)
			}

			c.ClockMu.Unlock()
			c.kvStoreMu.Unlock()

			// Then broadcast to all peers using direct message sending rather than Write
			for peerEthAddr := range c.Peers {
				if peerEthAddr == c.EthAddress {
					continue
				}

				c.ClockMu.RLock()
				clockCopy := c.Clock.Copy()
				c.ClockMu.RUnlock()

				reqID := uuid.New().String()
				requestPayload := &message.RequestMessage{
					Write: &message.WriteRequest{
						Key:   milestoneKey,
						Value: milestoneValue,
					},
				}

				msgToSend := message.NewRequestMessage(c.EthAddress, peerEthAddr, reqID, requestPayload, clockCopy, "")

				if err := c.signMessage(msgToSend); err != nil {
					fmt.Printf("Node %d: Failed to sign milestone message: %v\n", c.ID, err)
					continue
				}

				if err := c.sendMessage("", msgToSend); err != nil {
					fmt.Printf("Node %d: Failed to send milestone to %s: %v\n", c.ID, peerEthAddr, err)
				} else {
					fmt.Printf("Node %d: Broadcast milestone M:%d to %s\n", c.ID, epochNumber, peerEthAddr)
				}
			}
		}
	}
}

func (c *Client) handleIncomingMessages() {
	for msg := range c.network.IncomingMessages() {
		response := c.handleMessage(msg)
		if response != nil {
			response.Receiver = msg.Sender
			if err := c.sendMessage("", response); err != nil {
				fmt.Printf("Node %d [G:%d]: Failed send %s back to %s: %v\n", c.ID, goid(), response.Type, response.Receiver, err)
			} else {
				// fmt.Printf("Node %d [G:%d]: Sent %s successfully back to %s (ReqID: %s)\n", c.ID, goid(), response.Type, response.Receiver, response.ReqID) // Can be verbose
			}
		}
	}
	fmt.Printf("Node %d [G:%d]: Incoming message handler loop stopped.\n", c.ID, goid())
}

func (c *Client) handleDiscoveredPeers() {
	for pi := range c.network.DiscoveredPeers() {
		fmt.Printf("Node %d: Discovered peer %s with addrs %v\n", c.ID, pi.ID, pi.Addrs)
	}
	fmt.Printf("Node %d: Peer discovery handler loop stopped.\n", c.ID)
}

func (c *Client) signMessage(msg *message.Message) error {
	msg.Sender = c.EthAddress
	hash, err := msg.Hash()
	if err != nil {
		return fmt.Errorf("failed hash msg: %w", err)
	}
	signature, err := crypto.Sign(hash, c.PrivateKey)
	if err != nil {
		return fmt.Errorf("failed sign msg: %w", err)
	}
	s := new(big.Int).SetBytes(signature[32:64])
	secp256k1N := crypto.S256().Params().N
	halfN := new(big.Int).Rsh(secp256k1N, 1)
	if s.Cmp(halfN) > 0 {
		s.Sub(secp256k1N, s)
		sBytes := s.Bytes()
		copy(signature[64-len(sBytes):64], sBytes)
	}
	msg.Signature = hexutil.Encode(signature)
	return nil
}

func (c *Client) verifyMessageSignature(msg *message.Message) error {
	if msg.Signature == "" {
		return fmt.Errorf("no signature")
	}
	signature, err := hexutil.Decode(msg.Signature)
	if err != nil {
		return fmt.Errorf("decode sig: %w", err)
	}
	hash, err := msg.Hash()
	if err != nil {
		return fmt.Errorf("hash verify: %w", err)
	}
	if len(signature) != 65 {
		return fmt.Errorf("invalid sig length: %d, expected 65", len(signature))
	}
	pubKey, err := crypto.SigToPub(hash, signature)
	if err != nil {
		return fmt.Errorf("recover pubkey: %w", err)
	}
	recoveredAddr := crypto.PubkeyToAddress(*pubKey).Hex()
	if !strings.EqualFold(recoveredAddr, msg.Sender) {
		return fmt.Errorf("sig mismatch: got %s want %s", recoveredAddr, msg.Sender)
	}
	return nil
}

func (c *Client) Close() error {
	fmt.Printf("Node %d: Closing client...\n", c.ID)
	close(c.replyChan)
	return c.network.Close()
}
func (c *Client) GetStore() map[string]string {
	c.kvStoreMu.RLock()
	defer c.kvStoreMu.RUnlock()
	storeCopy := make(map[string]string)
	for k, v := range c.kvStore {
		storeCopy[k] = v
	}
	return storeCopy
}
func goid() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, _ := strconv.Atoi(idField)
	return id
}

// Records an event in the graph when a key-value pair is written
func (c *Client) recordMessageEvent(msg *message.Message, _ string) string {
	if msg == nil || msg.Request == nil || msg.Request.Write == nil {
		return ""
	}

	c.eventMu.Lock()
	defer c.eventMu.Unlock()

	key := msg.Request.Write.Key
	value := msg.Request.Write.Value

	if key == "" {
		return ""
	}

	eventName := fmt.Sprintf("%s:%s", key, value)

	// Return existing event if we've seen this key-value pair before
	if existingID, exists := c.lastMessageEvents[eventName]; exists {
		return existingID
	}

	parentIDs := make([]string, 0)

	// Determine the sender ID
	senderID := int(c.ID)
	if msg.Sender != c.EthAddress {
		if vlcID, exists := c.peerEthToID[msg.Sender]; exists {
			senderID = int(vlcID)
		}
	}

	// Handle milestone events (M:X:Y format)
	if strings.HasPrefix(key, "M") && key != "M0" {
		// Milestones should connect to the CAUSALLY LATEST KEY-VALUE EVENT of their epoch
		// Find the causally most recent non-milestone event using vector clocks
		latestKeyEventID := ""
		latestKeyEventClock := make(map[int]int)

		for storedName, eventID := range c.lastMessageEvents {
			// Skip milestone events, timestamp entries, clock entries, and latest_from_node entries
			if strings.HasPrefix(storedName, "M") ||
				strings.HasPrefix(storedName, "timestamp_") ||
				strings.HasPrefix(storedName, "clock_") ||
				strings.HasPrefix(storedName, "latest_from_node_") {
				continue
			}

			// This is a key-value event, get its vector clock
			clockKey := fmt.Sprintf("clock_%s", eventID)
			if clockStr, exists := c.lastMessageEvents[clockKey]; exists {
				eventClock := c.parseClockString(clockStr)

				// If this is the first key-value event we're checking, use it as baseline
				if latestKeyEventID == "" {
					latestKeyEventID = eventID
					latestKeyEventClock = eventClock
				} else {
					// Compare vector clocks to find the causally latest event
					// An event A is causally later than B if A's clock dominates B's clock
					isLater := true
					isEarlier := true

					// Check all nodes in both clocks
					allNodes := make(map[int]bool)
					for k := range eventClock {
						allNodes[k] = true
					}
					for k := range latestKeyEventClock {
						allNodes[k] = true
					}

					for nodeID := range allNodes {
						eventValue := eventClock[nodeID]           // defaults to 0 if not present
						latestValue := latestKeyEventClock[nodeID] // defaults to 0 if not present

						if eventValue < latestValue {
							isLater = false
						}
						if eventValue > latestValue {
							isEarlier = false
						}
					}

					// If this event is causally later (dominates the current latest), use it
					if isLater && !isEarlier {
						latestKeyEventID = eventID
						latestKeyEventClock = eventClock
					}
					// If events are concurrent (incomparable), we could use a tie-breaker
					// For now, we'll keep the first one found in concurrent cases
				}
			}
		}

		if latestKeyEventID != "" {
			parentIDs = append(parentIDs, latestKeyEventID)
			fmt.Printf("Node %d: Milestone %s connected to causally latest key-value event ID %s (clock: %v)\n",
				c.ID, eventName, latestKeyEventID, latestKeyEventClock)
		} else {
			// Fallback to genesis milestone if no key-value events found
			for storedName, eventID := range c.lastMessageEvents {
				if strings.HasPrefix(storedName, "M0:") {
					parentIDs = append(parentIDs, eventID)
					fmt.Printf("Node %d: Milestone %s connected to genesis M0 (no key events found)\n",
						c.ID, eventName)
					break
				}
			}
		}
	} else {
		// Handle regular key-value events
		// Strategy: Use vector clock causality to find immediate causal predecessors

		// Get the current event's vector clock
		currentClock := make(map[int]int)
		if msg.MsgClock != nil {
			for k, v := range msg.MsgClock.Values {
				currentClock[int(k)] = int(v)
			}
		}

		// Find immediate causal predecessors based on vector clock relationships
		var causalPredecessors []string

		// Check all existing events to see which ones are immediate causal predecessors
		for storedName, eventID := range c.lastMessageEvents {
			// Skip milestone events, timestamp entries, clock entries, and latest_from_node entries
			if strings.HasPrefix(storedName, "M") ||
				strings.HasPrefix(storedName, "timestamp_") ||
				strings.HasPrefix(storedName, "clock_") ||
				strings.HasPrefix(storedName, "latest_from_node_") {
				continue
			}

			// Get the stored event's vector clock
			clockKey := fmt.Sprintf("clock_%s", eventID)
			if clockStr, exists := c.lastMessageEvents[clockKey]; exists {
				storedClock := c.parseClockString(clockStr)

				// Check if this stored event is an immediate causal predecessor
				if c.isImmediateCausalPredecessor(storedClock, currentClock) {
					causalPredecessors = append(causalPredecessors, eventID)
				}
			}
		}

		// If we found causal predecessors, use them
		if len(causalPredecessors) > 0 {
			parentIDs = append(parentIDs, causalPredecessors...)
			fmt.Printf("Node %d: Key-value event %s connected to causal predecessors: %v\n",
				c.ID, eventName, causalPredecessors)
		} else {
			// No causal predecessors found, connect to the most appropriate milestone
			// Find the causally latest milestone using vector clocks, not timestamps
			mostRecentMilestone := ""
			mostRecentMilestoneClock := make(map[int]int)

			for storedName, eventID := range c.lastMessageEvents {
				if strings.HasPrefix(storedName, "M:") {
					// Get the milestone's vector clock
					clockKey := fmt.Sprintf("clock_%s", eventID)
					if clockStr, exists := c.lastMessageEvents[clockKey]; exists {
						milestoneClock := c.parseClockString(clockStr)

						// If this is the first milestone we're checking, use it as baseline
						if mostRecentMilestone == "" {
							mostRecentMilestone = eventID
							mostRecentMilestoneClock = milestoneClock
						} else {
							// Compare vector clocks to find the causally latest milestone
							isLater := true
							isEarlier := true

							// Check all nodes in both clocks
							allNodes := make(map[int]bool)
							for k := range milestoneClock {
								allNodes[k] = true
							}
							for k := range mostRecentMilestoneClock {
								allNodes[k] = true
							}

							for nodeID := range allNodes {
								milestoneValue := milestoneClock[nodeID]               // defaults to 0 if not present
								currentLatestValue := mostRecentMilestoneClock[nodeID] // defaults to 0 if not present

								if milestoneValue < currentLatestValue {
									isLater = false
								}
								if milestoneValue > currentLatestValue {
									isEarlier = false
								}
							}

							// If this milestone is causally later, use it
							if isLater && !isEarlier {
								mostRecentMilestone = eventID
								mostRecentMilestoneClock = milestoneClock
							}
						}
					}
				}
			}

			if mostRecentMilestone != "" {
				parentIDs = append(parentIDs, mostRecentMilestone)
				fmt.Printf("Node %d: Key-value event %s connected to causally latest milestone ID %s (clock: %v)\n",
					c.ID, eventName, mostRecentMilestone, mostRecentMilestoneClock)
			} else {
				// Connect to genesis milestone (M0)
				for storedName, eventID := range c.lastMessageEvents {
					if strings.HasPrefix(storedName, "M0:") {
						parentIDs = append(parentIDs, eventID)
						fmt.Printf("Node %d: Key-value event %s connected to genesis M0 ID %s (no milestones)\n",
							c.ID, eventName, eventID)
						break
					}
				}
			}
		}
	}

	// Final fallback - connect to genesis milestone (M0) if no parents found
	if len(parentIDs) == 0 && key != "M0" {
		for storedName, eventID := range c.lastMessageEvents {
			if strings.HasPrefix(storedName, "M0:") {
				parentIDs = append(parentIDs, eventID)
				break
			}
		}
	}

	// Convert vector clock format
	clock := make(map[int]int)
	if msg.MsgClock != nil {
		for k, v := range msg.MsgClock.Values {
			clock[int(k)] = int(v)
		}
	}

	// Add the event to the graph
	fmt.Printf("Node %d: Creating CHRONO event %s with parents: %v\n", c.ID, eventName, parentIDs)
	eventID := c.EventGraph.AddEvent(eventName, key, value, clock, parentIDs)

	// Track this event for future reference
	c.lastMessageEvents[eventName] = eventID

	// Update latest event pointer for this sender
	latestFromSenderKey := fmt.Sprintf("latest_from_node_%d", senderID)
	c.lastMessageEvents[latestFromSenderKey] = eventID

	// Store vector clock for the event for causality checking
	clockKey := fmt.Sprintf("clock_%s", eventID)
	clockStr := fmt.Sprintf("%v", clock)
	c.lastMessageEvents[clockKey] = clockStr

	// Update milestone tracking if needed
	if strings.HasPrefix(key, "M") {
		c.lastMilestone = eventID
		c.milestoneCreated = true
	}

	return eventID
}

// SetEpochThreshold sets the number of messages required to trigger an epoch finalization
func (c *Client) SetEpochThreshold(threshold int) {
	if threshold <= 0 {
		return
	}
	c.epochMu.Lock()
	c.epochThreshold = threshold
	c.epochMu.Unlock()
	fmt.Printf("Node %d: Set epoch threshold to %d messages\n", c.ID, threshold)
}

// trackMessageForEpoch adds a message to the current epoch tracking and checks if an epoch should be finalized
func (c *Client) trackMessageForEpoch(msg *message.Message) {
	if msg == nil || msg.ReqID == "" {
		return
	}

	if msg.Type == message.TypeP2P && msg.P2P != nil && msg.P2P.Data != "" {
		if strings.Contains(msg.P2P.Data, "epoch_finalize") ||
			strings.Contains(msg.P2P.Data, "epoch_vote") {
			return
		}
	}

	isWrite := msg.Type == message.TypeRequest && msg.Request != nil && msg.Request.Write != nil

	c.epochMu.Lock()
	defer c.epochMu.Unlock()

	digest := fmt.Sprintf("%s:%s:%s", msg.ReqID, msg.Sender, msg.Type)

	c.epochMessageIDs = append(c.epochMessageIDs, msg.ReqID)
	c.epochMessageDigests = append(c.epochMessageDigests, digest)

	if isWrite {
		c.epochMessageCount++
		fmt.Printf("Node %d: Epoch message count now %d/%d (after processing write)\n",
			c.ID, c.epochMessageCount, c.epochThreshold)

		// Check if we've reached the threshold
		if c.epochMessageCount >= c.epochThreshold {
			fmt.Printf("Node %d: Reached threshold of %d write messages - finalizing epoch\n",
				c.ID, c.epochThreshold)
			c.finalizeEpoch()
		}
	}
}

// finalizeEpoch creates and broadcasts a milestone event when epoch threshold is reached
func (c *Client) finalizeEpoch() {
	// Calculate hash of all messages in this epoch
	hasher := sha256.New()

	// Sort digests for consistent hashing regardless of message order
	sort.Strings(c.epochMessageDigests)

	// Add the last epoch hash for chaining epochs
	if c.lastEpochHash != "" {
		hasher.Write([]byte(c.lastEpochHash))
	}

	// Add all message digests
	for _, digest := range c.epochMessageDigests {
		hasher.Write([]byte(digest))
	}

	epochHash := hex.EncodeToString(hasher.Sum(nil))
	currentEpochNumber := c.epochNumber + 1

	// Create the milestone key and value
	milestoneKey := fmt.Sprintf("M:%d", currentEpochNumber)
	milestoneValue := fmt.Sprintf("%d", c.ID)

	fmt.Printf("Node %d: FINALIZING EPOCH %d with %d messages, creating milestone %s\n",
		c.ID, currentEpochNumber, c.epochMessageCount, milestoneKey)

	// Create and store the milestone locally first
	c.kvStoreMu.Lock()
	c.ClockMu.Lock()

	if _, exists := c.kvStore[milestoneKey]; !exists {
		c.kvStore[milestoneKey] = milestoneValue
		c.Clock.Inc(c.ID)

		// Create a message for the milestone event
		clockCopy := c.Clock.Copy()

		dummyMsg := &message.Message{
			Request: &message.RequestMessage{
				Write: &message.WriteRequest{
					Key:   milestoneKey,
					Value: milestoneValue,
				},
			},
			MsgClock: clockCopy,
			Sender:   c.EthAddress,
		}

		// Record the milestone event in the graph
		c.recordMessageEvent(dummyMsg, "")

		fmt.Printf("Node %d: Created local milestone %s:%s\n", c.ID, milestoneKey, milestoneValue)
	}

	c.ClockMu.Unlock()
	c.kvStoreMu.Unlock()

	// Reset for next epoch
	c.epochNumber = currentEpochNumber
	c.epochMessageCount = 0
	c.epochMessageIDs = make([]string, 0)
	c.epochMessageDigests = make([]string, 0)
	c.lastEpochHash = epochHash

	// Now broadcast the milestone to all peers using Write messages
	for peerEthAddr := range c.Peers {
		if peerEthAddr == c.EthAddress {
			continue
		}

		// Get current clock for the message
		c.ClockMu.RLock()
		clockCopy := c.Clock.Copy()
		c.ClockMu.RUnlock()

		reqID := uuid.New().String()
		requestPayload := &message.RequestMessage{
			Write: &message.WriteRequest{
				Key:   milestoneKey,
				Value: milestoneValue,
			},
		}

		msgToSend := message.NewRequestMessage(c.EthAddress, peerEthAddr, reqID, requestPayload, clockCopy, "")

		if err := c.signMessage(msgToSend); err != nil {
			fmt.Printf("Node %d: Failed to sign milestone message: %v\n", c.ID, err)
			continue
		}

		if err := c.sendMessage("", msgToSend); err != nil {
			fmt.Printf("Node %d: Failed to send milestone to %s: %v\n", c.ID, peerEthAddr, err)
		} else {
			fmt.Printf("Node %d: Broadcast milestone %s to %s\n", c.ID, milestoneKey, peerEthAddr)
		}
	}
}

func (c *Client) parseClockString(clockStr string) map[int]int {
	eventClock := make(map[int]int)

	// Simple parsing of clock format like "map[1:5 2:3]"
	clockStr = strings.TrimPrefix(clockStr, "map[")
	clockStr = strings.TrimSuffix(clockStr, "]")

	if clockStr != "" {
		pairs := strings.Split(clockStr, " ")
		for _, pair := range pairs {
			if strings.Contains(pair, ":") {
				parts := strings.Split(pair, ":")
				if len(parts) == 2 {
					if key, err := strconv.Atoi(parts[0]); err == nil {
						if value, err := strconv.Atoi(parts[1]); err == nil {
							eventClock[key] = value
						}
					}
				}
			}
		}
	}

	return eventClock
}

// isImmediateCausalPredecessor checks if storedClock is an immediate causal predecessor of currentClock
func (c *Client) isImmediateCausalPredecessor(storedClock, currentClock map[int]int) bool {
	// For an event to be an immediate causal predecessor, it should be:
	// 1. Exactly one step behind in the vector clock (not just any "happened before")
	// 2. From the same node or a directly related communication

	// Count how many clock values differ and by how much
	differences := 0
	totalDifference := 0

	// Check all nodes in both clocks
	allNodes := make(map[int]bool)
	for k := range storedClock {
		allNodes[k] = true
	}
	for k := range currentClock {
		allNodes[k] = true
	}

	for nodeID := range allNodes {
		storedValue := storedClock[nodeID]   // defaults to 0 if not present
		currentValue := currentClock[nodeID] // defaults to 0 if not present

		if storedValue > currentValue {
			// Stored event is in the future relative to current - not a predecessor
			return false
		}

		if currentValue > storedValue {
			differences++
			totalDifference += (currentValue - storedValue)
		}
	}

	// For immediate causality, we want:
	// - Exactly 1 or 2 clock positions to differ (indicating direct communication)
	// - Total difference should be small (1-3 steps max)
	return differences > 0 && differences <= 2 && totalDifference <= 3
}
