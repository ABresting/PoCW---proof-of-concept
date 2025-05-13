package client

import (
	"crypto/ecdsa"
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
		hasher: fnv.New64a(),
	}
	go client.handleIncomingMessages()
	go client.handleDiscoveredPeers()
	return client, nil
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
		fmt.Printf("Node %d [G:%d]: N%d.Write PRE-MSG-CREATE: Intended Key='%s', Value='%s'\n", c.ID, goid(), c.ID, key, value)
		msgToSend = message.NewRequestMessage(c.EthAddress, peerEthAddr, reqID, requestPayload, newClock, "")
		if msgToSend.Request != nil && msgToSend.Request.Write != nil {
			fmt.Printf("Node %d [G:%d]: N%d.Write POST-MSG-CREATE: Msg Key='%s', Value='%s'\n", c.ID, goid(), c.ID, msgToSend.Request.Write.Key, msgToSend.Request.Write.Value)
		}
		if err := c.signMessage(msgToSend); err != nil {
			c.logMu.Unlock()
			c.ClockMu.Unlock()
			c.kvStoreMu.Unlock()
			return fmt.Errorf("failed sign local write msg %s: %w", key, err)
		}
		c.messageLog = append(c.messageLog, msgToSend)
		fmt.Printf("Node %d [G:%d]: Logged write msg for key '%s'. Log size: %d\n", c.ID, goid(), key, len(c.messageLog))
	} else {
		fmt.Printf("Node %d [G:%d]: Key '%s' already exists locally. Write aborted (no send).\n", c.ID, goid(), key)
	}
	c.logMu.Unlock()
	c.ClockMu.Unlock()
	c.kvStoreMu.Unlock()
	if msgToSend != nil {
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
				fmt.Printf("Node %d [G:%d]: Self-read %s got ReadReply. CausallyReady: %t, Found: %t\n", c.ID, goid(), reqID, rr.CausallyReady, rr.Found)
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
				fmt.Printf("Node %d [G:%d]: Received matching read reply for %s\n", c.ID, goid(), reqID)
				if reply.Reply == nil || reply.Reply.ReadReply == nil {
					return false, "", fmt.Errorf("read reply %s nil body", reqID)
				}
				c.ClockMu.Lock()
				c.Clock.Merge([]*vlc.Clock{reply.MsgClock})
				c.ClockMu.Unlock()
				fmt.Printf("Node %d [G:%d]: Updated clock post-read reply %s: %v\n", c.ID, goid(), reqID, c.Clock.Values)
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

	remoteSenderEntryValue := remoteClock.Values[senderVLCID] // Should exist in a valid write message from sender

	fmt.Printf("Node %d [G:%d]: checkCanApplyDirectWrite: LocalClk %v, RemoteClk %v (SenderID %d: LocalVal %d, RemoteVal %d)\n",
		c.ID, goid(), localClock.Values, remoteClock.Values, senderVLCID, localSenderEntryValue, remoteSenderEntryValue)

	// Rule 1: First meaningful message from this sender?
	// (Local clock has 0 for sender, remote has >0 for sender)
	if localSenderEntryValue == 0 && remoteSenderEntryValue > 0 {
		// Additional check: Is remoteClock *only* ahead for this sender?
		// Or is it okay if it's ahead for others too for the very first message?
		// For simplicity now: If it's the first we've heard from them with their clock, accept it as baseline.
		// We still need to ensure the remote clock isn't *behind* us on other entries if we have them.
		isAheadOnOtherEntries := false
		for id, rVal := range remoteClock.Values {
			if id == senderVLCID {
				continue
			}
			lVal := localClock.Values[id]
			if rVal > lVal {
				isAheadOnOtherEntries = true
				break
			}
		}
		if !isAheadOnOtherEntries {
			fmt.Printf("Node %d [G:%d]: Apply rule: FIRST_MESSAGE from sender %d. Remote clock %v.\n", c.ID, goid(), senderVLCID, remoteClock.Values)
			return 1, false // Apply as first message
		} else {
			fmt.Printf("Node %d [G:%d]: Apply rule: FIRST_MESSAGE from sender %d, but remote %v is ahead on other entries vs local %v. Requires sync.\n", c.ID, goid(), senderVLCID, remoteClock.Values, localClock.Values)
			return 0, true // Potentially complex state, request full sync
		}
	}

	// Rule 3: Strict +1 Increment (if not the first message)
	isPlusOne := localClock.IsPlusOneIncrement(remoteClock, senderVLCID)
	if isPlusOne {
		fmt.Printf("Node %d [G:%d]: Apply rule: PLUS_ONE increment from sender %d.\n", c.ID, goid(), senderVLCID)
		return 2, false // Apply as +1 increment
	}

	// Rule 2: Gap Detected / Behind generally (if not +1 and not first special case)
	comparison := localClock.Compare(remoteClock)
	if comparison == vlc.Less {
		fmt.Printf("Node %d [G:%d]: Apply rule: GAP_DETECTED (local < remote, not +1). Requires Sync.\n", c.ID, goid())
		return 0, true // Requires sync
	}

	// Otherwise (local clock is Equal, Greater, or Incomparable but not a clear "behind" case)
	fmt.Printf("Node %d [G:%d]: Apply rule: IGNORE (local %v vs remote %v, comp %d).\n", c.ID, goid(), localClock.Values, remoteClock.Values, comparison)
	return 0, false // Ignore, no sync needed from this message
}

// handleRequest processes incoming Request messages
func (c *Client) handleRequest(msg *message.Message) *message.Message {
	if msg.Request == nil || msg.MsgClock == nil { // MsgClock must be present for writes from sender
		fmt.Printf("Node %d [G:%d]: Received invalid request (nil body or clock).\n", c.ID, goid())
		return nil
	}
	fmt.Printf("Node %d [G:%d]: Handling Request %s from %s, MsgClock: %v\n", c.ID, goid(), msg.ReqID, msg.Sender, msg.MsgClock.Values)
	if err := c.verifyMessageSignature(msg); err != nil {
		fmt.Printf("Invalid req sig: %v\n", err)
		return nil
	}

	var responseToSend *message.Message = nil

	// --- Handle READ ---
	if msg.Request.Read != nil {
		key := msg.Request.Read.Key
		fmt.Printf("Node %d [G:%d]: Processing Read Req %s key '%s'\n", c.ID, goid(), msg.ReqID, key)
		// For reads, the incoming msg.MsgClock is the *reader's* clock. We check if *our* state is sufficient.
		_, localReadyForReader := c.checkCausalConsistency(msg.MsgClock)

		c.kvStoreMu.RLock()
		value, found := c.kvStore[key]
		c.kvStoreMu.RUnlock()
		c.ClockMu.RLock()
		currentLocalClock := c.Clock.Copy()
		c.ClockMu.RUnlock() // Our current clock for the reply

		// Reply with current state, indicating if we were causally ready for *their* request clock
		replyBody := &message.ReplyMessage{ReadReply: &message.ReadReply{Key: key, Value: value, Found: found, CausallyReady: localReadyForReader}}
		responseToSend = message.NewReplyMessage(c.EthAddress, msg.Sender, msg.ReqID, replyBody.ReadReply, currentLocalClock, "")

		if !localReadyForReader { // If our state is not sufficient for their read's causal requirements
			fmt.Printf("Node %d [G:%d]: Not causally ready for Read Req %s from %s. Sending SyncRequest instead of (or in addition to) Reply.\n", c.ID, goid(), msg.ReqID, msg.Sender)
			// For reads, it's usually better to reply with what we have and indicate not ready.
			// SyncRequest could be a follow-up by the reader if they desire.
			// This sends SyncRequest, commenting out the line below time to avoid complex cases.
			// syncReqMsg := message.NewSyncRequestMessage(c.EthAddress, msg.Sender, msg.ReqID, currentLocalClock, "")
			// responseToSend = syncReqMsg // This would replace the actual read reply.
		}

		// --- Handle WRITE ---
	} else if msg.Request.Write != nil {
		key := msg.Request.Write.Key
		value := msg.Request.Write.Value
		remoteClock := msg.MsgClock // This is the clock of the sender *after* they applied the write

		fmt.Printf("Node %d [G:%d]: In handleRequest(Write) PRE-checkCanApplyDirectWrite for msg from Sender '%s'.\n", c.ID, goid(), msg.Sender)
		c.ClockMu.RLock() // Safely read peerEthToID
		fmt.Printf("Node %d [G:%d]: Current peerEthToID map: %v\n", c.ID, goid(), c.peerEthToID)
		c.ClockMu.RUnlock()

		applyAction, requiresSync := c.checkCanApplyDirectWrite(msg)

		if applyAction > 0 { // Apply if apply_as_first (1) or apply_as_plus_one (2)
			fmt.Printf("Node %d [G:%d]: Write Req %s key '%s' meets apply condition (Action: %d). Processing...\n", c.ID, goid(), msg.ReqID, key, applyAction)
			c.kvStoreMu.Lock()
			c.ClockMu.Lock()
			c.logMu.Lock()
			_, exists := c.kvStore[key]
			if !exists {
				// Actual write to kvStore
				fmt.Printf("Node %d [G:%d]: APPLYING write key '%s' = '%s'. Local clock %v merging remote %v\n", c.ID, goid(), key, value, c.Clock.Values, remoteClock.Values)
				c.kvStore[key] = value
				c.Clock.Merge([]*vlc.Clock{remoteClock}) // Merge sender's post-write clock
				// Log the received message
				foundInLog := false
				for _, logged := range c.messageLog {
					if logged.ReqID == msg.ReqID && logged.Sender == msg.Sender {
						foundInLog = true
						break
					}
				}
				if !foundInLog {
					c.messageLog = append(c.messageLog, msg)
				} else {
					// Here we can implement a more sophisticated check if needed to
					// punish the double-senders i.e. same clock value but different
					// messages.
					fmt.Printf("Node %d [G:%d]: Write from %s (ReqID %s) already in log. Not re-adding.\n", c.ID, goid(), msg.Sender, msg.ReqID)
				}
				fmt.Printf("Node %d [G:%d]: DONE Apply+Log. Clock: %v. Log size: %d\n", c.ID, goid(), c.Clock.Values, len(c.messageLog))
			} else {
				fmt.Printf("Node %d [G:%d]: Key '%s' exists. Write from %s ignored. Merging clock.\n", c.ID, goid(), key, msg.Sender)
				c.Clock.Merge([]*vlc.Clock{remoteClock}) // Still merge clock for info
			}
			c.logMu.Unlock()
			c.ClockMu.Unlock()
			c.kvStoreMu.Unlock()
			responseToSend = nil // No response for successful direct writes

		} else if requiresSync {
			fmt.Printf("Node %d [G:%d]: Write Req %s needs sync. Sending SyncRequest.\n", c.ID, goid(), msg.ReqID)
			c.ClockMu.RLock()
			localClockCopy := c.Clock.Copy()
			c.ClockMu.RUnlock()
			syncReqMsg := message.NewSyncRequestMessage(c.EthAddress, msg.Sender, msg.ReqID, localClockCopy, "")
			responseToSend = syncReqMsg
		} else { // applyAction == 0 && !requiresSync (e.g., local clock is ahead or incomparable in a non-behind way (parallel state))
			fmt.Printf("Node %d [G:%d]: Ignoring Write Req %s (e.g. Local Clock >= Remote).\n", c.ID, goid(), msg.ReqID)
			c.ClockMu.Lock()
			c.Clock.Merge([]*vlc.Clock{remoteClock})
			c.ClockMu.Unlock() // Merge clock for knowledge
			responseToSend = nil
		}
	}

	// Sign the response if generated (Read Reply or SyncRequest)
	if responseToSend != nil {
		if err := c.signMessage(responseToSend); err != nil {
			fmt.Printf("Failed sign response %s: %v\n", msg.ReqID, err)
			return nil
		}
		fmt.Printf("Node %d [G:%d]: Prepared %s for ReqID %s to %s\n", c.ID, goid(), responseToSend.Type, msg.ReqID, responseToSend.Receiver)
	} else {
		fmt.Printf("Node %d [G:%d]: No response generated for ReqID %s\n", c.ID, goid(), msg.ReqID)
	}
	return responseToSend
}

// handleSyncRequest sends back missing messages from the log
func (c *Client) handleSyncRequest(msg *message.Message) *message.Message {
	if msg.SyncRequest == nil || msg.SyncRequest.RequesterClock == nil {
		fmt.Printf("Node %d [G:%d]: Invalid SyncRequest (nil body or requester clock).\n", c.ID, goid())
		return nil
	}
	requesterClock := msg.SyncRequest.RequesterClock
	fmt.Printf("Node %d [G:%d]: Handling SyncRequest from %s (ReqID: %s) who has clock %v\n", c.ID, goid(), msg.Sender, msg.ReqID, requesterClock.Values)
	if err := c.verifyMessageSignature(msg); err != nil {
		fmt.Printf("Node %d [G:%d]: Invalid signature on SyncRequest from %s: %v\n", c.ID, goid(), msg.Sender, err)
		return nil
	}

	c.logMu.RLock()   // Lock for reading messageLog
	c.ClockMu.RLock() // Lock for reading local Clock
	// Create a snapshot of the log to iterate over without holding lock for too long
	localLogSnapshot := make([]*message.Message, len(c.messageLog))
	copy(localLogSnapshot, c.messageLog)
	localClockCopy := c.Clock.Copy() // Copy of current clock to send with response
	c.ClockMu.RUnlock()
	c.logMu.RUnlock()

	missingMessages := make([]*message.Message, 0)
	// Compare our current overall clock with the requester's clock
	comparisonWithRequesterOverall := localClockCopy.Compare(requesterClock)

	// We should send messages if our overall state is ahead or incomparable
	if comparisonWithRequesterOverall == vlc.Greater || comparisonWithRequesterOverall == vlc.Incomparable {
		fmt.Printf("Node %d [G:%d]: Comparing local log (size %d) against requester clock %v\n", c.ID, goid(), len(localLogSnapshot), requesterClock.Values)
		for _, logMsg := range localLogSnapshot {
			if logMsg.MsgClock == nil { // Should not happen with proper logging
				continue
			}
			// A message is "missing" for the requester if their clock is causally before the message's clock
			// i.e., logMsg.MsgClock is NOT (Less Than or Equal To) requesterClock
			logMsgComparisonToRequester := logMsg.MsgClock.Compare(requesterClock)
			if !(logMsgComparisonToRequester == vlc.Less || logMsgComparisonToRequester == vlc.Equal) {
				keyDbg := "?"
				valDbg := "?"
				if logMsg.Request != nil && logMsg.Request.Write != nil {
					keyDbg = logMsg.Request.Write.Key
					valDbg = logMsg.Request.Write.Value
				}
				fmt.Printf("Node %d [G:%d]: N%d.handleSyncRequest: Adding to missingMessages: Key='%s', Value='%s', ReqID='%s', MsgClock=%v (for %s)\n",
					c.ID, goid(), c.ID, keyDbg, valDbg, logMsg.ReqID, logMsg.MsgClock.Values, msg.Sender)
				missingMessages = append(missingMessages, logMsg)
			}
		}
	}

	if len(missingMessages) > 0 {
		fmt.Printf("Node %d [G:%d]: Sending %d missing messages in SyncResponse to %s.\n", c.ID, goid(), len(missingMessages), msg.Sender)
		// Sort messages by their MsgClock to help receiver process them in order
		sort.SliceStable(missingMessages, func(i, j int) bool {
			if missingMessages[i] == nil || missingMessages[i].MsgClock == nil {
				return true
			} // Nils first
			if missingMessages[j] == nil || missingMessages[j].MsgClock == nil {
				return false
			}
			comp := missingMessages[i].MsgClock.Compare(missingMessages[j].MsgClock)
			return comp == vlc.Less // Sort by clock, Less comes first
		})
		syncRespMsg := message.NewSyncResponseMessage(c.EthAddress, msg.Sender, msg.ReqID, missingMessages, localClockCopy, "")
		if err := c.signMessage(syncRespMsg); err != nil {
			fmt.Printf("Node %d [G:%d]: Failed to sign SyncResponse: %v\n", c.ID, goid(), err)
			return nil
		}
		return syncRespMsg
	} else {
		fmt.Printf("Node %d [G:%d]: No missing messages found for %s based on their clock. No SyncResponse sent.\n", c.ID, goid(), msg.Sender)
		return nil
	}
}

// handleSyncResponse processes received missing messages
func (c *Client) handleSyncResponse(msg *message.Message) {
	if msg.SyncResponse == nil || msg.MsgClock == nil { // Outer msg.MsgClock is sender's current clock at time of sending sync resp
		fmt.Printf("Node %d [G:%d]: Invalid SyncResponse message (nil body or clock).\n", c.ID, goid())
		return
	}
	missingMessages := msg.SyncResponse.MissingMessages
	fmt.Printf("Node %d [G:%d]: Handling SyncResponse from %s (ReqID: %s) with %d messages. Sender's Current Clock %v\n",
		c.ID, goid(), msg.Sender, msg.ReqID, len(missingMessages), msg.MsgClock.Values)

	if err := c.verifyMessageSignature(msg); err != nil {
		fmt.Printf("Node %d [G:%d]: Invalid signature on SyncResponse from %s: %v\n", c.ID, goid(), msg.Sender, err)
		return
	}

	// Merge sender's overall clock from the SyncResponse message itself.
	c.ClockMu.Lock()
	c.Clock.Merge([]*vlc.Clock{msg.MsgClock})
	initialMergedClock := c.Clock.Copy() // For logging
	c.ClockMu.Unlock()
	fmt.Printf("Node %d [G:%d]: Clock after merging SyncResponse sender's current clock: %v\n", c.ID, goid(), initialMergedClock.Values)

	processedCount := 0
	// Sort received messages by their *internal* MsgClock to attempt processing in causal order.
	sort.SliceStable(missingMessages, func(i, j int) bool {
		if missingMessages[i] == nil || missingMessages[i].MsgClock == nil {
			return true
		}
		if missingMessages[j] == nil || missingMessages[j].MsgClock == nil {
			return false
		}
		comp := missingMessages[i].MsgClock.Compare(missingMessages[j].MsgClock)
		return comp == vlc.Less
	})

	for i, msgToApply := range missingMessages {
		if msgToApply == nil || msgToApply.Request == nil || msgToApply.Request.Write == nil || msgToApply.MsgClock == nil {
			fmt.Printf("Node %d [G:%d]: Skipping invalid message content at index [%d] in SyncResponse\n", c.ID, goid(), i)
			continue
		}

		key := msgToApply.Request.Write.Key
		val := msgToApply.Request.Write.Value
		messageSpecificClock := msgToApply.MsgClock // Clock from when sender applied *this* msg

		fmt.Printf("Node %d [G:%d]: N%d.handleSyncResponse: Processing synced msg [%d] EXTRACTED Key='%s', Value='%s' (ReqID: %s) SpecificClock %v\n",
			c.ID, goid(), c.ID, i, key, val, msgToApply.ReqID, messageSpecificClock.Values)

		// Lock resources for check and potential update
		c.kvStoreMu.Lock()
		c.ClockMu.Lock()
		c.logMu.Lock()

		localClockBeforeApplySpecificMsg := c.Clock.Copy() // Our *current* clock before this specific message
		_, keyExistsLocally := c.kvStore[key]
		comparisonWithMsgSpecificClock := localClockBeforeApplySpecificMsg.Compare(messageSpecificClock)

		// Apply if:
		// 1. Key does NOT exist locally.
		// 2. Our current local clock is NOT strictly GREATER than the specific message's clock.
		if !keyExistsLocally && comparisonWithMsgSpecificClock != vlc.Greater {
			fmt.Printf("Node %d [G:%d]: N%d.handleSyncResponse: APPLYING synced message for new key '%s' = '%s'. Local clk %v merging remote specific clk %v (CompToMsgSpecific=%d)\n",
				c.ID, goid(), c.ID, key, val, localClockBeforeApplySpecificMsg.Values, messageSpecificClock.Values, comparisonWithMsgSpecificClock)

			c.kvStore[key] = val
			c.Clock.Merge([]*vlc.Clock{messageSpecificClock}) // Merge this specific message's clock

			foundInLog := false
			for _, loggedMsg := range c.messageLog {
				if loggedMsg.ReqID == msgToApply.ReqID && loggedMsg.Sender == msgToApply.Sender { // A simple check
					foundInLog = true
					break
				}
			}
			if !foundInLog {
				c.messageLog = append(c.messageLog, msgToApply)
				processedCount++
				fmt.Printf("Node %d [G:%d]: Clock after apply & merge synced msg: %v. Log size %d\n", c.ID, goid(), c.Clock.Values, len(c.messageLog))
			} else {
				fmt.Printf("Node %d [G:%d]: Synced msg key '%s' (ReqID %s) already in log. Clock merged.\n", c.ID, goid(), key, msgToApply.ReqID)
			}
		} else {
			logReason := ""
			if keyExistsLocally {
				logReason += "KeyExistsLocally "
			}
			if comparisonWithMsgSpecificClock == vlc.Greater {
				logReason += "LocalClockStrictlyAheadOfMsgSpecificClock "
			}
			fmt.Printf("Node %d [G:%d]: N%d.handleSyncResponse: Skipping synced msg key '%s': %s. Attempting to merge specific msg clock.\n", c.ID, goid(), c.ID, key, logReason)

			// If skipped K/V, still merge the clock from message if it provides new information
			if comparisonWithMsgSpecificClock == vlc.Less || comparisonWithMsgSpecificClock == vlc.Incomparable {
				c.Clock.Merge([]*vlc.Clock{messageSpecificClock})
				fmt.Printf("Node %d [G:%d]: Clock (no K/V apply) after merging specific msg clock: %v\n", c.ID, goid(), c.Clock.Values)
			}
		}
		c.logMu.Unlock()
		c.ClockMu.Unlock()
		c.kvStoreMu.Unlock()
	}

	// Re-sort entire message log AFTER potentially adding multiple items
	c.logMu.Lock()
	sort.SliceStable(c.messageLog, func(i, j int) bool {
		if c.messageLog[i] == nil || c.messageLog[i].MsgClock == nil {
			return true
		}
		if c.messageLog[j] == nil || c.messageLog[j].MsgClock == nil {
			return false
		}
		comp := c.messageLog[i].MsgClock.Compare(c.messageLog[j].MsgClock)
		return comp == vlc.Less
	})
	c.logMu.Unlock()

	c.ClockMu.RLock()
	finalClockVal := c.Clock.Values
	c.ClockMu.RUnlock()
	c.logMu.RLock()
	finalLogSize := len(c.messageLog)
	c.logMu.RUnlock()
	fmt.Printf("Node %d [G:%d]: Finished SyncResponse. Applied %d updates. Final Clock: %v. Log size: %d\n",
		c.ID, goid(), processedCount, finalClockVal, finalLogSize)
}

func (c *Client) handleMessage(msg *message.Message) *message.Message {
	if msg == nil {
		return nil
	}
	clockStr := "nil"
	if msg.MsgClock != nil {
		clockStr = fmt.Sprintf("%v", msg.MsgClock.Values)
	}
	fmt.Printf("Node %d [G:%d]: Received message Type=%s from %s ReqID=%s Clock=%s\n", c.ID, goid(), msg.Type, msg.Sender, msg.ReqID, clockStr)
	var response *message.Message
	switch msg.Type {
	case message.TypeP2P:
		if msg.P2P != nil && msg.MsgClock != nil {
			if err := c.verifyMessageSignature(msg); err == nil {
				fmt.Printf("Node %d [G:%d]: Received P2P MsgID %s\n", c.ID, goid(), msg.P2P.MsgID)
				c.ClockMu.Lock()
				c.Clock.Merge([]*vlc.Clock{msg.MsgClock})
				c.ClockMu.Unlock()
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
		fmt.Printf("Node %d [G:%d]: Received Read Reply from %s ReqID %s. Forwarding.\n", c.ID, goid(), msg.Sender, msg.ReqID)
		if err := c.verifyMessageSignature(msg); err == nil {
			select {
			case c.replyChan <- msg:
			default:
				fmt.Printf("WARN: Reply chan full %s\n", msg.ReqID)
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

func (c *Client) handleIncomingMessages() {
	fmt.Printf("Node %d [G:%d]: Starting incoming message handler loop.\n", c.ID, goid())
	for msg := range c.network.IncomingMessages() {
		response := c.handleMessage(msg)
		if response != nil {
			response.Receiver = msg.Sender
			if err := c.sendMessage("", response); err != nil {
				fmt.Printf("Node %d [G:%d]: Failed send %s back to %s: %v\n", c.ID, goid(), response.Type, response.Receiver, err)
			} else {
				fmt.Printf("Node %d [G:%d]: Sent %s successfully back to %s (ReqID: %s)\n", c.ID, goid(), response.Type, response.Receiver, response.ReqID)
			}
		}
	}
	fmt.Printf("Node %d [G:%d]: Incoming message handler loop stopped.\n", c.ID, goid())
}

// handleDiscoveredPeers (Restored - Placeholder)
func (c *Client) handleDiscoveredPeers() {
	fmt.Printf("Node %d: Starting peer discovery handler loop.\n", c.ID)
	for pi := range c.network.DiscoveredPeers() {
		fmt.Printf("Node %d: Discovered peer %s with addrs %v\n", c.ID, pi.ID, pi.Addrs)
		// TODO: Handshake for EthAddr/VLC ID -> AddPeerMapping
	}
	fmt.Printf("Node %d: Peer discovery handler loop stopped.\n", c.ID)
}

// --- Cryptography Helpers ---

// signMessage (Restored)
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

// verifyMessageSignature (Restored)
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
	if len(signature) != 65 && len(signature) != 64 {
		return fmt.Errorf("invalid sig length: %d", len(signature))
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
	close(c.replyChan)       // Signal sendRequest loops to exit
	return c.network.Close() // Close libp2p host
}

// GetStore returns a copy for printing/logging the state
func (c *Client) GetStore() map[string]string {
	c.kvStoreMu.RLock()
	defer c.kvStoreMu.RUnlock()
	storeCopy := make(map[string]string)
	for k, v := range c.kvStore {
		storeCopy[k] = v
	}
	return storeCopy
}

// Helper to get current goroutine ID for debugging
func goid() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, _ := strconv.Atoi(idField)
	return id
}
