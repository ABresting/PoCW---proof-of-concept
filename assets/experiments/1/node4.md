# Node 4 Event Graph - Step by Step

This explains exactly how each event gets added to Node 4's graph and what it connects to.

## Step 1: M0:4 (Genesis)
- **Added**: When Node 4 starts up
- **Connects to**: Nothing (it's the first event)
- **Why**: Every node creates its own genesis milestone when it joins

## Step 2: k3:v3
- **Added**: Node 4 receives this event from Node 3
- **Connects to**: M0:4
- **Why**: When Node 4 receives an event from another node, it connects it to the most recent milestone (M0:4)

## Step 3: k4:v4
- **Added**: Node 4 writes k4=v4 and sends to Node 1
- **Connects to**: k3:v3
- **Why**: This is Node 4's first local event, and it happened after receiving k3:v3, so it connects to it

## Step 4: k8:v8
- **Added**: Node 4 writes k8=v8 and sends to Node 1
- **Connects to**: k4:v4
- **Why**: This continues Node 4's local chain of events after k4:v4

## Step 5: k12:v12
- **Added**: Node 4 writes k12=v12 and sends to Node 1
- **Connects to**: M0:4
- **Why**: This event connects back to the genesis milestone, showing it may have been processed independently or had different causal timing

## Step 6: M:1:1 (First Milestone)
- **Added**: Node 4 receives this milestone from Node 1 (Node 1 created it)
- **Connects to**: k3:v3, k4:v4, k8:v8, k12:v12 (ALL events from the first epoch)
- **Why**: Milestones connect to ALL events that happened since the last milestone

## Step 7: k16:v16
- **Added**: Node 4 writes k16=v16 and sends to Node 1
- **Connects to**: M:1:1
- **Why**: This is the first event after the milestone, so it connects to the milestone

## Step 8: k20:v20
- **Added**: Node 4 writes k20=v20 and sends to Node 1
- **Connects to**: k16:v16
- **Why**: This continues Node 4's local chain after k16:v16

## Step 9: k24:v24
- **Added**: Node 4 writes k24=v24 and sends to Node 1
- **Connects to**: k16:v16
- **Why**: This event connects to k16:v16, showing it's part of the same epoch but may have different causal relationship than k20:v20

## Step 10: k28:v28
- **Added**: Node 4 writes k28=v28 and sends to Node 1
- **Connects to**: k24:v24
- **Why**: This continues from k24:v24, forming a branch in Node 4's local timeline

## Step 11: M:2:2 (Second Milestone)
- **Added**: Node 4 receives this milestone from Node 2 (Node 2 created it)
- **Connects to**: k16:v16, k20:v20, k24:v24, k28:v28 (ALL events from the second epoch)
- **Why**: Milestones connect to ALL events that happened since the previous milestone

## Simple Rules

1. **Local events** (events Node 4 creates) connect to previous local events or milestones based on causal timing
2. **Received events** (from other nodes) connect to the most recent milestone or to local events if they have causal relationship
3. **Milestones** connect to ALL events that happened since the previous milestone
4. **First event after genesis** connects to M0:4 or to received events
5. **Events can branch** - multiple events can connect to the same parent, creating different causal paths

## Key Observations for Node 4

- Node 4 has branching local chains: k4:v4 → k8:v8 and k16:v16 → k20:v20 and k16:v16 → k24:v24 → k28:v28
- Node 4 received k3:v3 from Node 3 and connected it to its genesis
- Node 4 received both milestones (M:1:1 and M:2:2) from other nodes
- k12:v12 connects directly to M0:4, showing independent processing
- k24:v24 and k20:v20 both connect to k16:v16, creating parallel branches
- The graph shows Node 4's role as both a receiver of events and creator of branching local event chains
- Node 4 demonstrates the most complex branching pattern among all nodes 