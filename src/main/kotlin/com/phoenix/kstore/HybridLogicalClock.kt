package com.phoenix.kstore

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

typealias PackedHLCTimestamp = Long

class HLCTimestamp : Comparable<HLCTimestamp> {

    val ts: Long
    val count: Int

    companion object {

        const val COUNT_MAX = 10000L
    }

    constructor(ts: Long, count: Int) {
        this.ts = ts
        this.count = count
    }

    constructor(packed: PackedHLCTimestamp)  {
        ts = packed / COUNT_MAX
        count = (packed % COUNT_MAX).toInt()
    }

    /**
     * Convenient format for storing HLCTimestamp
     * @throws RuntimeException if count exceeds 4 digit precision max
     */
    fun pack(): PackedHLCTimestamp {
        if (count > 9999) throw RuntimeException("count exceeded precision max")
        return (ts * COUNT_MAX) + count
    }


    override fun compareTo(other: HLCTimestamp): Int {
        if (ts == other.ts) {
            if (count == other.count) {
                return 0
            }
            return count - other.count
        }
        return (ts - other.ts).toInt()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        return this.compareTo(other as HLCTimestamp) == 0
    }

    override fun hashCode(): Int {
        var result = ts.hashCode()
        result = 31 * result + count
        return result
    }
}

class HybridLogicalClock {

    private var ts = System.currentTimeMillis()
    private var count = 0
    private val mutex = Mutex()

    suspend fun receive(incoming: HLCTimestamp) = mutex.withLock {
        val now = System.currentTimeMillis()

        if (now > ts && now > incoming.ts) {
            ts = now
            count = 0
        } else if (ts == incoming.ts) {
            count = maxOf(count, incoming.count)
        } else if (ts > incoming.ts) {
            count += 1
        } else {
            ts = incoming.ts
            count = incoming.count + 1
        }
    }

    suspend fun increment(): HLCTimestamp = mutex.withLock {
        val now = System.currentTimeMillis()

        if (now > ts)
            ts = now
        else
            count += 1

        HLCTimestamp(ts, count)
    }
}
