# Node 2 Event Graph - Step by Step

This explains exactly how each event gets added to Node 2's graph and what it connects to.

## Step 1: M0:2 (Genesis)
- **Added**: When Node 2 starts up
- **Connects to**: Nothing (it's the first event)
- **Why**: Every node creates its own genesis milestone when it joins

## Step 2: k1:v1
- **Added**: Node 2 receives this event from Node 1
- **Connects to**: M0:2
- **Why**: When Node 2 receives an event from another node, it connects it to the most recent milestone (M0:2)

## Step 3: k2:v2
- **Added**: Node 2 writes k2=v2 and sends to Node 3
- **Connects to**: k1:v1
- **Why**: This is Node 2's first local event, and it happened after receiving k1:v1, so it connects to it

## Step 4: k6:v6
- **Added**: Node 2 writes k6=v6 and sends to Node 3
- **Connects to**: k2:v2
- **Why**: This continues Node 2's local chain of events after k2:v2

## Step 5: k10:v10
- **Added**: Node 2 writes k10=v10 and sends to Node 3
- **Connects to**: k6:v6
- **Why**: This continues Node 2's local chain of events after k6:v6

## Step 6: M:1:1 (First Milestone)
- **Added**: Node 2 receives this milestone from Node 1 (Node 1 created it)
- **Connects to**: k1:v1, k2:v2, k6:v6, k10:v10 (ALL events from the first epoch)
- **Why**: Milestones connect to ALL events that happened since the last milestone

## Step 7: k14:v14
- **Added**: Node 2 writes k14=v14 and sends to Node 3
- **Connects to**: M:1:1
- **Why**: This is the first event after the milestone, so it connects to the milestone

## Step 8: k18:v18
- **Added**: Node 2 writes k18=v18 and sends to Node 3
- **Connects to**: k14:v14
- **Why**: This continues Node 2's local chain after k14:v14

## Step 9: k22:v22
- **Added**: Node 2 writes k22=v22 and sends to Node 3
- **Connects to**: k18:v18
- **Why**: This continues Node 2's local chain after k18:v18

## Step 10: k26:v26
- **Added**: Node 2 writes k26=v26 and sends to Node 3
- **Connects to**: k22:v22
- **Why**: This continues Node 2's local chain after k22:v22

## Step 11: k30:v30
- **Added**: Node 2 writes k30=v30 and sends to Node 3
- **Connects to**: k26:v26
- **Why**: This continues Node 2's local chain after k26:v26

## Step 12: M:2:2 (Second Milestone)
- **Added**: Node 2 creates this milestone after enough write messages in the network
- **Connects to**: k14:v14, k18:v18, k22:v22, k26:v26, k30:v30 (ALL events from the second epoch)
- **Why**: Milestones connect to ALL events that happened since the previous milestone

## Simple Rules

1. **Local events** (events Node 2 creates) connect to the previous local event in Node 2's timeline
2. **Received events** (from other nodes) connect to the most recent milestone or to local events if they have causal relationship
3. **Milestones** connect to ALL events that happened since the previous milestone
4. **First event after genesis** connects to M0:2 or to received events
5. **First event after any milestone** connects to that milestone

## Key Observations for Node 2

- Node 2 has a clear local chain: k2:v2 → k6:v6 → k10:v10 → M:1:1 → k14:v14 → k18:v18 → k22:v22 → k26:v26 → k30:v30 → M:2:2
- Node 2 received k1:v1 from Node 1 and connected it to its genesis
- Node 2 created the second milestone (M:2:2) which connects to all events in the second epoch
- The graph shows Node 2's role as both a receiver and creator of events in the distributed system 