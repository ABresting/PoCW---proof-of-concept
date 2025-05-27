# Node 1 Event Graph - Step by Step

This explains exactly how each event gets added to Node 1's graph and what it connects to.

## Step 1: M0:1 (Genesis)
- **Added**: When Node 1 starts up
- **Connects to**: Nothing (it's the first event)
- **Why**: Every node creates its own genesis milestone when it joins

## Step 2: k1:v1 
- **Added**: Node 1 writes k1=v1 and sends to Node 2
- **Connects to**: M0:1
- **Why**: This is Node 1's first real event, so it connects to the genesis milestone

## Step 3: k5:v5
- **Added**: Node 1 writes k5=v5 and sends to Node 2  
- **Connects to**: k1:v1
- **Why**: This happened after k1:v1 in Node 1's timeline, so it connects to the previous local event

## Step 4: k4:v4
- **Added**: Node 1 receives this event from Node 4
- **Connects to**: M0:1
- **Why**: When Node 1 receives an event from another node, it connects it to the most recent milestone (M0:1)

## Step 5: k9:v9
- **Added**: Node 1 writes k9=v9 and sends to Node 2
- **Connects to**: k5:v5
- **Why**: This continues Node 1's local chain of events after k5:v5

## Step 6: M:1:1 (First Milestone)
- **Added**: Node 1 creates this milestone after 5 write messages in the network
- **Connects to**: k1:v1, k5:v5, k9:v9, k4:v4 (ALL events from the first epoch)
- **Why**: Milestones connect to ALL events that happened since the last milestone

## Step 7: k13:v13
- **Added**: Node 1 writes k13=v13 and sends to Node 2
- **Connects to**: M:1:1
- **Why**: This is the first event after the milestone, so it connects to the milestone

## Step 8: k17:v17
- **Added**: Node 1 writes k17=v17 and sends to Node 2
- **Connects to**: k13:v13
- **Why**: This continues Node 1's local chain after k13:v13

## Step 9: k21:v21
- **Added**: Node 1 writes k21=v21 and sends to Node 2
- **Connects to**: k17:v17
- **Why**: This continues Node 1's local chain after k17:v17

## Step 10: M:2:2 (Second Milestone)
- **Added**: Node 1 receives this milestone from Node 2 (Node 2 created it)
- **Connects to**: k13:v13, k17:v17, k21:v21 (and other events from second epoch)
- **Why**: Milestones connect to ALL events that happened since the previous milestone

## Step 11: k25:v25
- **Added**: Node 1 writes k25=v25 and sends to Node 2
- **Connects to**: M:2:2
- **Why**: This is the first event after the second milestone, so it connects to that milestone

## Step 12: k29:v29
- **Added**: Node 1 writes k29=v29 and sends to Node 2
- **Connects to**: k25:v25
- **Why**: This continues Node 1's local chain after k25:v25

## Simple Rules

1. **Local events** (events Node 1 creates) connect to the previous local event in Node 1's timeline
2. **Received events** (from other nodes) connect to the most recent milestone
3. **Milestones** connect to ALL events that happened since the previous milestone
4. **First event after genesis** connects to M0:1
5. **First event after any milestone** connects to that milestone 