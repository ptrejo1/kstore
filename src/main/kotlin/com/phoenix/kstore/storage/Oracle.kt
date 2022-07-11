package com.phoenix.kstore.storage

import com.phoenix.kstore.AbortTransactionException
import com.phoenix.kstore.OverflowException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Transaction status oracle. Enforces isolation level and transaction ordering.
 * Transactions aren't thread safe, but operations in this class must be.
 */
class Oracle {

    val writeMutex = Mutex()

    private var nextTs = 1L
    private val commits = hashMapOf<String, Long>()
    private val mutex = Mutex()

    /**
     * if a transaction gets a commit timestamp of 1
     * then its snapshot of the db includes everything that occurred until 0
     */
    suspend fun readTs(): Long = mutex.withLock {
        nextTs - 1
    }

    /**
     * per ssi - abort transaction if there are any writes that have occurred since
     * this transaction started that affect keys read by this transaction, then keep
     * track of this transaction's writes for other transactions to do the same.
     */
    suspend fun commitRequest(transaction: Transaction): Long = mutex.withLock {
        for (key in transaction.reads) {
            val lastCommit = commits[key]
            if (lastCommit != null && lastCommit > transaction.readTs)
                throw AbortTransactionException()
        }

        val ts = nextTs
        nextTs += 1

        if (ts == Long.MAX_VALUE)
            throw OverflowException()

        transaction.writes.keys.forEach {
            commits[it] = ts
        }

        ts
    }
}
