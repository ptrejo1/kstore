package com.phoenix.kstore

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.*

class LWWRegister(val replicaId: String) {

    var addSet = LinkedHashMap<String, PackedHLCTimestamp>()
    var removeSet = LinkedHashMap<String, PackedHLCTimestamp>()

    private val clock = HybridLogicalClock()
    private val mutex = Mutex()

    suspend fun add(element: String) = mutex.withLock {
        addSet[element] = clock.increment().pack()
    }

    suspend fun remove(element: String) = mutex.withLock {
        removeSet[element] = clock.increment().pack()
    }

    suspend fun merge(incoming: LWWRegister) = mutex.withLock {
        merge(incoming.addSet, addSet)
        merge(incoming.removeSet, removeSet)
    }

    private suspend fun merge(
        incomingSet: LinkedHashMap<String, PackedHLCTimestamp>,
        intoSet: LinkedHashMap<String, PackedHLCTimestamp>
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
