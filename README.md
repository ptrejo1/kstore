# kstore
A distributed key value store I created to learn more about distributed systems.

### Membership
kstore uses a slightly simplified version of the [SWIM](https://research.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)(Scalable Weakly-consistent Infection-style Process Group Membership Protocol) protocol
to determine what the current members of the cluster are and disseminate this info among the joined nodes. To maintain cluster state within a node, 
kstore uses a type of CRDT (conflict-free replicated data type) called a [LWW Register](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type#LWW-Element-Set_(Last-Write-Wins-Element-Set)).
The LWW register maintains 2 dictionaries, an add set and a remove set, with the key being the node name + ip address and value
a [HLC](http://muratbuffalo.blogspot.com/2014/07/hybrid-logical-clocks.html) (hybrid logical clock). The HLC is used to facilitate maintaining a 
total order of events. Each node's current state is disseminated through the cluster by the protocol's gossip loop, so that all the node's will eventually 
converge on the correct state.

### Storage
For the storage layer, I wanted to write something that supported ACIDish (everything just gets stored in memory) 
transactions and [MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control). The db is composed of two main components the 
oracle and the memtable. The oracle serves as the point of entry for transactions by maintaining read/write timestamps for transactions
and tracking dependent keys to support SSI. The memtable actually stores the data, with an AVL Tree being used as the index and 
the data itself just being stored in an array of bytes.

#### Isolation Level
For transactions, I chose to go with [SSI](https://drkp.net/papers/ssi-vldb12.pdf) (serializable snapshot isolation). SSI builds on top of SI (snapshot isolation), 
in which transactions get assigned a timestamp and are only allowed to see data from transactions that have been 
committed prior to that timestamp. By performing some booking on transaction dependencies, SSI is able to prevent against
some anomalies like write skew that are possible with SI.

### API
kstore exposes a very simple API composed of put, get, and delete operations, which can be grouped into transactions.
Keys are routed to and assigned to nodes using the distributed hashing algorithm described in the [Google Maglev](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/44824.pdf) paper.

In addition, I created a simple grammar for issuing commands to the server.
```
put <key> <value>;
get <key>;
delete <key>;
begin <operation>... end;
info;
```
Can be used from `Client.kt` after starting a node.

### Links
* https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf
* https://dgraph.io/blog/post/badger-txn/
* https://drkp.net/papers/ssi-vldb12.pdf
* https://www.cockroachlabs.com/blog/consistency-model/
* https://en.wikipedia.org/wiki/Multiversion_concurrency_control
* http://www.cs.cornell.edu/Projects/ladis2009/papers/lakshman-ladis2009.pdf
* https://jaredforsyth.com/posts/hybrid-logical-clocks/
* http://muratbuffalo.blogspot.com/2014/07/hybrid-logical-clocks.html
* https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type#LWW-Element-Set_(Last-Write-Wins-Element-Set)
* http://book.mixu.net/distsys/eventual.html
* https://research.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf
* https://blog.kevingomez.fr/2019/01/29/clusters-and-membership-discovering-the-swim-protocol/
* https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/44824.pdf
