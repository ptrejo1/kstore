package com.phoenix.kstore.storage

import com.phoenix.kstore.AbortTransactionException
import com.phoenix.kstore.OverflowException

class Store(private val maxTableSize: Int = 1024 shl 20) {

    val oracle = Oracle()
    val memTable = MemTable(maxTableSize)

    /**
     * Get from store
     */
    suspend fun get(key: ByteArray): ByteArray? = withTransaction { txn ->
        txn.read(key)
    }

    /**
     * Put key value pair in store
     * @throws [AbortTransactionException]
     * @throws [OverflowException]
     */
    suspend fun put(key: ByteArray, value: ByteArray) = withTransaction { txn ->
        txn.write(key, value)
    }

    /**
     * Delete from store
     * @throws [OverflowException]
     */
    suspend fun delete(key: ByteArray) = withTransaction { txn ->
        txn.write(key, byteArrayOf(), EntryMeta.TOMBSTONED.value)
    }

    /**
     * Called by transactions to read from db
     */
    fun read(key: ByteArray): Entry? = memTable.get(key)

    /**
     * Called by transactions to submit writes
     */
    fun write(entries: List<Entry>) {
        entries.forEach { memTable.put(it) }
    }

    private suspend fun <R> withTransaction(operation: (transaction: Transaction) -> R): R {
        val txn = Transaction(this)
        val r = operation(txn)
        txn.commit()
        return r
    }
}
