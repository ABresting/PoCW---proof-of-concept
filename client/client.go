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
		msgToSend = message.NewRequestMessage(c.EthAddress, peerEthAddr, reqID, requestPayload, newClock, "")
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

		applyAction, requiresSync := c.checkCanApplyDirectWrite(msg)

		c.ClockMu.Lock()
		if applyAction > 0 { // applyAction 1 (first) or 2 (+one)

			c.Clock.Merge([]*vlc.Clock{remoteClock})

			c.Clock.Inc(c.ID)

			currentClockAfterUpdateValues := c.Clock.Copy().Values // For general logging
			c.kvStoreMu.Lock()
			c.logMu.Lock()
			_, exists := c.kvStore[key]
			if !exists || applyAction == 1 || applyAction == 2 {
				c.kvStore[key] = value
				fmt.Printf("Node %d [G:%d]: APPLYING write key '%s' = '%s'. Action: %d. New Clock: %v\n", c.ID, goid(), key, value, applyAction, currentClockAfterUpdateValues)
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
	// fmt.Printf("Node %d [G:%d]: Handling SyncRequest from %s (ReqID: %s) who has clock %v\n", c.ID, goid(), msg.Sender, msg.ReqID, requesterClock.Values) // Can be verbose
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
			fmt.Printf("Node %d [G:%d]: Failed to sign SyncResponse: %v\n", c.ID, goid(), err)
			return nil
		}
		return syncRespMsg
	}
	// fmt.Printf("Node %d [G:%d]: No missing messages for %s. No SyncResponse sent.\n", c.ID, goid(), msg.Sender) // Can be verbose
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
	// fmt.Printf("Node %d [G:%d]: Handling SyncResponse from %s with %d msgs. Sender's Clock %v\n", c.ID, goid(), msg.Sender, len(missingMessages), msg.MsgClock.Values) // Can be verbose
	c.ClockMu.Lock()
	c.Clock.Merge([]*vlc.Clock{msg.MsgClock})
	c.Clock.Inc(c.ID) /* initialMergedClockValues := c.Clock.Copy().Values; */
	c.ClockMu.Unlock()
	// fmt.Printf("Node %d [G:%d]: Clock after merging SyncResponse outer clock & inc: %v\n", c.ID, goid(), initialMergedClockValues) // Can be verbose
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
	sort.SliceStable(c.messageLog, func(i, j int) bool { return c.messageLog[i].MsgClock.Compare(missingMessages[j].MsgClock) == vlc.Less })
	c.logMu.Unlock()
	c.ClockMu.RLock()
	finalClockVal := c.Clock.Values
	c.ClockMu.RUnlock()
	c.logMu.RLock()
	finalLogSize := len(c.messageLog)
	c.logMu.RUnlock()
	fmt.Printf("Node %d [G:%d]: Finished SyncResponse. Processed %d updates. Final Clock: %v. Log size: %d\n", c.ID, goid(), processedCount, finalClockVal, finalLogSize)
}

func (c *Client) handleMessage(msg *message.Message) *message.Message {
	if msg == nil {
		return nil
	}
	// clockStr := "nil"; if msg.MsgClock != nil { clockStr = fmt.Sprintf("%v", msg.MsgClock.Values) } // Can be verbose
	// fmt.Printf("Node %d [G:%d]: Received message Type=%s from %s ReqID=%s Clock=%s\n", c.ID, goid(), msg.Type, msg.Sender, msg.ReqID, clockStr) // Can be verbose
	var response *message.Message
	switch msg.Type {
	case message.TypeP2P:
		if msg.P2P != nil && msg.MsgClock != nil {
			if err := c.verifyMessageSignature(msg); err == nil {
				c.ClockMu.Lock()
				c.Clock.Merge([]*vlc.Clock{msg.MsgClock})
				c.Clock.Inc(c.ID)
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
