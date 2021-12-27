package com.phoenix.kstore

import com.phoenix.kstore.utils.NodeKey
import com.phoenix.kstore.utils.NodeKeyRepr
import com.phoenix.kstore.utils.NodeName
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.*

class LWWRegister(val nodeName: NodeName) {

    var addSet = LinkedHashMap<NodeKeyRepr, PackedHLCTimestamp>()
    var removeSet = LinkedHashMap<NodeKeyRepr, PackedHLCTimestamp>()

    val state: List<Pair<NodeKeyRepr, PackedHLCTimestamp>>
        get() {
            return addSet
                .filter { !removeSet.containsKey(it.key) || removeSet[it.key]!! <= it.value }
                .map { it.key to it.value }
        }

    private val clock = HybridLogicalClock()
    private val mutex = Mutex()

    suspend fun add(nodeKey: NodeKey) = mutex.withLock {
        addSet[nodeKey.toString()] = clock.increment().pack()
    }

    suspend fun remove(nodeKey: NodeKey) = mutex.withLock {
        removeSet[nodeKey.toString()] = clock.increment().pack()
    }

    suspend fun merge(incoming: LWWRegister) = mutex.withLock {
        merge(incoming.addSet, addSet)
        merge(incoming.removeSet, removeSet)
    }

    private suspend fun merge(
        incomingSet: LinkedHashMap<NodeKeyRepr, PackedHLCTimestamp>,
        intoSet: LinkedHashMap<NodeKeyRepr, PackedHLCTimestamp>
    ) {
        // Using the mutex of this class in this functions body will create a deadlock
        // Coroutine locks are non-reentrant

        for ((elem, packed) in incomingSet) {
            val existing = intoSet[elem]
            if (existing == null) {
                intoSet[elem] = packed
                continue
            }

            val incomingTimestamp = HLCTimestamp(packed)
            val existingTimestamp = HLCTimestamp(existing)
            clock.receive(incomingTimestamp)

            if (incomingTimestamp > existingTimestamp)
                intoSet[elem] = packed
        }
    }
}
