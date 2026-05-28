# Graft

An implementation of <a href="https://raft.github.io/raft.pdf">Raft</a> consensus algorithm in Go.

This project is my first time building with Go so the code could be sloppy & beginner level.

## Architecture (WIP)

The clients will call a simple Gateway service (stable endpoint pointing to the gateway pod running inside K8s) that will in turn call the leader pod inside the k8s StatefulSet. Internally, there will be N nodes that will be in consensus using the Raft Algorithm & each having their own DB as a place to store the data (will keep it simple like a KV store).
Each Pod will have their own Log file that will store the operations requested by the client(s).

So client calls the gateway service, which in turn calls the gateway pod and which calls the leader pod in the K8s StatefulSet.

<i>
[Current state - basic Raft implemented with gateway & nodes locally.
<br/>
Next step - Adding DB to the nodes to persist the data & removing in-memory KVStore.]
</i>

```mermaid
flowchart LR
    C[Client] --> SVC[Gateway Service]
    SVC --> GW[Gateway Pod<br/>Leader-aware router]

    subgraph K8S[Kubernetes Cluster]
        GW

        subgraph RAFT[Raft Peer StatefulSet]
            direction LR

            subgraph P0[peer-0]
                R0[Raft Node]
                DB0[(Embedded DB)]
                PV0[(Persistent Volume)]
                R0 --> DB0
                DB0 --> PV0
            end

            subgraph P1[peer-1]
                R1[Raft Node]
                DB1[(Embedded DB)]
                PV1[(Persistent Volume)]
                R1 --> DB1
                DB1 --> PV1
            end

            subgraph P2[peer-2]
                R2[Raft Node]
                DB2[(Embedded DB)]
                PV2[(Persistent Volume)]
                R2 --> DB2
                DB2 --> PV2
            end
        end

        HS[Headless Service<br/>Stable peer DNS]
    end

    GW -->|Forward client request to current leader| R0
    GW -. Health checks / leader discovery .-> R0
    GW -. Health checks / leader discovery .-> R1
    GW -. Health checks / leader discovery .-> R2

    R0 <--> |AppendEntries / RequestVote| R1
    R1 <--> |AppendEntries / RequestVote| R2
    R2 <--> |AppendEntries / RequestVote| R0

    HS --- R0
    HS --- R1
    HS --- R2
```

## Learning

My main objective is to learn Go as a language, understand its syntax, paradigms and how it works overall by implementing the Raft algorithm.
Also, I would learn the devOps side as well with deployments, configuration and cluster mangement (even though on a simpler level) with Docker and Kubernetes.