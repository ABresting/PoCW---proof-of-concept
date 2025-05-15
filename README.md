# Proof-of-Concept for PoCW (Proof-of-Causal-Work) 

This PoC is done using three nodes that communicate with one another using the libp2p as the networking layer. Each node maintains store data structure to capture the events and a vlc clock holder mapping node id to the clock value [NodeID] -> [ClockValue].

The code is developed and has been tested on the following platforms:
- Ubuntu 24.04.2 LTS
- Libp2p (for networking between nodes)
- Ethereum signature scheme

## Requirements

- Golang >= go1.23.8
- Libp2p

## Execution

``` bash
# Go to the directory where the code is located
go run .
```

## Explanation

Node 1, Node 2, and Node 3 are the three nodes having Ethereum addresses as their libp2p IDs. Each node has a vlc clock holder that maps the node ID number to the clock value. For eg. For Node with with ID 1, may have the clock values as C = {1: 4}, that is Node 1 has clock value 4. 

The test run scenario is as follows:

1. Node 1 sends message k1:v1 to Node 2
2. Node 1 sends message k2:v2 to Node 2
3. Node 1 sends message k3:v3 to Node 3
4. Node 1 sends message k4:v4 to Node 2
5. Node 2 sends message k5:v5 to Node 3

Eventually the nodes will have the following vlc clock and store values:

Node 1:
  Clock: map[1:4]
  Store: map[k1:v1 k2:v2 k3:v3 k4:v4]

Node 2:
  Clock: map[1:4 2:4]
  Store: map[k1:v1 k2:v2 k5:v5]

Node 3:
  Clock: map[1:4 2:4 3:2]
  Store: map[k3:v3 k5:v5]

## Note - Proof of Causal Work - Blockchain integration

The entities involved can generate a store/graph's mutation proof using a SNARK. The proof can be thus be proposed along with the block/transaction proposal. Now other Nodes in the system if agree with proof and proposal can send their approval vote . This block/transaction can then be verified using a smart contract by checking signatures of other Nodes already registered on the blockchain smart contract.

    --- A Node proposes a transaction/block
        |
        |---> (SNARK Proof + Transaction/Block proposal) ----Send----> Other Nodes to Vote
        |

        --- Other Nodes vote ---

        If Votes above a threshold, eg. 2f+1 in BFT setting?
        |
        |---> (SNARK Proof + Transaction/Block proposal + Votes) --> (proposed Block/Transaction)
        |
        |---> Send to Blockchain Smart Contract
        |
        |---> If verified, add to blockchain

This logic can be implemented using a smart contract irrespective of the underlying blockchain. The smart contract can be implemented for EVM or SVM or MOVE or any other compilation framework that is supported by the blockchain platform.