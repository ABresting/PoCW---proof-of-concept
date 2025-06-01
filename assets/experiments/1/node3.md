# Node 3 Event Graph - Step by Step

This explains exactly how each event gets added to Node 3's graph and what it connects to.

## Step 1: M0:3 (Genesis)
- **Added**: When Node 3 starts up
- **Connects to**: Nothing (it's the first event)
- **Why**: Every node creates its own genesis milestone when it joins

## Step 2: k2:v2
- **Added**: Node 3 receives this event from Node 2
- **Connects to**: M0:3
- **Why**: When Node 3 receives an event from another node, it connects it to the most recent milestone (M0:3)

## Step 3: k3:v3
- **Added**: Node 3 writes k3=v3 and sends to Node 4
- **Connects to**: k2:v2
- **Why**: This is Node 3's first local event, and it happened after receiving k2:v2, so it connects to it

## Step 4: k7:v7
- **Added**: Node 3 writes k7=v7 and sends to Node 4
- **Connects to**: k3:v3
- **Why**: This continues Node 3's local chain of events after k3:v3

## Step 5: k11:v11
- **Added**: Node 3 writes k11=v11 and sends to Node 4
- **Connects to**: k7:v7
- **Why**: This continues Node 3's local chain of events after k7:v7

## Step 6: M:1:1 (First Milestone)
- **Added**: Node 3 receives this milestone from Node 1 (Node 1 created it)
- **Connects to**: k2:v2, k3:v3, k7:v7, k11:v11 (ALL events from the first epoch)
- **Why**: Milestones connect to ALL events that happened since the last milestone

## Step 7: k15:v15
- **Added**: Node 3 writes k15=v15 and sends to Node 4
- **Connects to**: M:1:1
- **Why**: This is the first event after the milestone, so it connects to the milestone

## Step 8: k19:v19
- **Added**: Node 3 writes k19=v19 and sends to Node 4
- **Connects to**: k15:v15
- **Why**: This continues Node 3's local chain after k15:v15

## Step 9: k23:v23
- **Added**: Node 3 writes k23=v23 and sends to Node 4
- **Connects to**: k19:v19
- **Why**: This continues Node 3's local chain after k19:v19

## Step 10: k27:v27
- **Added**: Node 3 writes k27=v27 and sends to Node 4
- **Connects to**: k15:v15
- **Why**: This event connects to k15:v15, showing it's part of the same epoch but may have different causal relationship

## Step 11: M:2:2 (Second Milestone)
- **Added**: Node 3 receives this milestone from Node 2 (Node 2 created it)
- **Connects to**: k15:v15, k19:v19, k23:v23, k27:v27 (ALL events from the second epoch)
- **Why**: Milestones connect to ALL events that happened since the previous milestone

## Simple Rules

1. **Local events** (events Node 3 creates) connect to the previous local event in Node 3's timeline
2. **Received events** (from other nodes) connect to the most recent milestone or to local events if they have causal relationship
3. **Milestones** connect to ALL events that happened since the previous milestone
4. **First event after genesis** connects to M0:3 or to received events
5. **First event after any milestone** connects to that milestone

## Key Observations for Node 3

- Node 3 has clear local chains: k3:v3 → k7:v7 → k11:v11 and k15:v15 → k19:v19 → k23:v23
- Node 3 received k2:v2 from Node 2 and connected it to its genesis
- Node 3 received both milestones (M:1:1 and M:2:2) from other nodes
- k27:v27 connects to k15:v15 instead of following the k19→k23 chain, showing different causal relationships
- The graph shows Node 3's role primarily as a receiver of milestones and creator of local event chains
- Node 3 demonstrates how events can have different parent relationships even within the same epoch 