package message

import (
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/yourorg/p2p-framework/vlc"
)

// MessageType defines the type of message
type MessageType string

const (
	TypeP2P          MessageType = "p2p"           // Legacy P2P
	TypeRequest      MessageType = "request"       // Request (Read/Write)
	TypeReply        MessageType = "reply"         // Reply ONLY FOR READS
	TypeSyncRequest  MessageType = "sync_request"  // Request for missing historical messages
	TypeSyncResponse MessageType = "sync_response" // Response containing historical messages
	TypeTerminate    MessageType = "terminate"     // Termination
)

// Message is the top-level message wrapper
type Message struct {
	Type         MessageType          `json:"type"`
	P2P          *P2PMessage          `json:"p2p,omitempty"`
	Request      *RequestMessage      `json:"request,omitempty"`
	Reply        *ReplyMessage        `json:"reply,omitempty"` // Only for Reads now
	SyncRequest  *SyncRequestMessage  `json:"sync_request,omitempty"`
	SyncResponse *SyncResponseMessage `json:"sync_response,omitempty"`
	Terminate    bool                 `json:"terminate,omitempty"`
	Sender       string               `json:"sender"`
	Receiver     string               `json:"receiver"`
	ReqID        string               `json:"req_id,omitempty"`
	MsgClock     *vlc.Clock           `json:"msg_clock,omitempty"` // Clock state when message was created/sent
	Signature    string               `json:"signature"`
}

// P2PMessage
type P2PMessage struct {
	MsgID string `json:"msg_id"`
	Data  string `json:"data"`
}

// RequestMessage
type RequestMessage struct {
	Read  *ReadRequest  `json:"read,omitempty"`
	Write *WriteRequest `json:"write,omitempty"`
}

// ReadRequest
type ReadRequest struct {
	Key string `json:"key"`
}

// WriteRequest
type WriteRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ReplyMessage
type ReplyMessage struct {
	ReadReply  *ReadReply  `json:"read_reply,omitempty"`
	WriteReply *WriteReply `json:"write_reply,omitempty"`
}

// ReadReply
type ReadReply struct {
	Key           string `json:"key"`
	Value         string `json:"value"`
	Found         bool   `json:"found"`
	CausallyReady bool   `json:"causally_ready"`
}

// WriteReply
type WriteReply struct {
	Key           string `json:"key"`
	Added         bool   `json:"added"`
	CausallyReady bool   `json:"causally_ready"`
}

// SyncRequestMessage
type SyncRequestMessage struct {
	RequesterClock *vlc.Clock `json:"requester_clock"`
}

// SyncResponseMessage - Contains historical messages the requester is missing
type SyncResponseMessage struct {
	MissingMessages []*Message `json:"missing_messages"`
}

// --- Constructors ---

func NewP2PMessage(sender, receiver, msgID, data, signature string, clock *vlc.Clock) *Message {
	return &Message{Type: TypeP2P, P2P: &P2PMessage{MsgID: msgID, Data: data}, Sender: sender, Receiver: receiver, ReqID: msgID, MsgClock: clock, Signature: signature}
}
func NewRequestMessage(sender, receiver, reqID string, request *RequestMessage, clock *vlc.Clock, signature string) *Message {
	return &Message{Type: TypeRequest, Request: request, Sender: sender, Receiver: receiver, ReqID: reqID, MsgClock: clock, Signature: signature}
}

// NewReplyMessage - ONLY for Read Replies
func NewReplyMessage(sender, receiver, reqID string, readReply *ReadReply, clock *vlc.Clock, signature string) *Message {
	reply := &ReplyMessage{ReadReply: readReply}
	return &Message{Type: TypeReply, Reply: reply, Sender: sender, Receiver: receiver, ReqID: reqID, MsgClock: clock, Signature: signature}
}
func NewSyncRequestMessage(sender, receiver, reqID string, localClock *vlc.Clock, signature string) *Message {
	return &Message{Type: TypeSyncRequest, SyncRequest: &SyncRequestMessage{RequesterClock: localClock}, Sender: sender, Receiver: receiver, ReqID: reqID, MsgClock: localClock, Signature: signature}
}
func NewSyncResponseMessage(sender, receiver, reqID string, missingMsgs []*Message, clock *vlc.Clock, signature string) *Message {
	return &Message{Type: TypeSyncResponse, SyncResponse: &SyncResponseMessage{MissingMessages: missingMsgs}, Sender: sender, Receiver: receiver, ReqID: reqID, MsgClock: clock, Signature: signature}
}

func NewTerminateMessage(sender, receiver, reqID, signature string) *Message {
	return &Message{Type: TypeTerminate, Terminate: true, Sender: sender, Receiver: receiver, ReqID: reqID, Signature: signature}
}

func (m *Message) Hash() ([]byte, error) {
	type messageForHash struct {
		Type         MessageType          `json:"type"`
		P2P          *P2PMessage          `json:"p2p,omitempty"`
		Request      *RequestMessage      `json:"request,omitempty"`
		Reply        *ReplyMessage        `json:"reply,omitempty"`
		SyncRequest  *SyncRequestMessage  `json:"sync_request,omitempty"`
		SyncResponse *SyncResponseMessage `json:"sync_response,omitempty"`
		Terminate    bool                 `json:"terminate,omitempty"`
		Sender       string               `json:"sender"`
		Receiver     string               `json:"receiver"`
		ReqID        string               `json:"req_id,omitempty"`
		MsgClock     *vlc.Clock           `json:"msg_clock,omitempty"`
	}
	// Ensure order matches struct definition for consistency
	data, err := json.Marshal(messageForHash{m.Type, m.P2P, m.Request, m.Reply, m.SyncRequest, m.SyncResponse, m.Terminate, m.Sender, m.Receiver, m.ReqID, m.MsgClock})
	if err != nil {
		return nil, fmt.Errorf("marshal hash: %v", err)
	}
	msgToSign := fmt.Sprintf("\x19Ethereum Signed Message:\n%d%s", len(data), data)
	return crypto.Keccak256([]byte(msgToSign)), nil
}
