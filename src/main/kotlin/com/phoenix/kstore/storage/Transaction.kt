package com.phoenix.kstore.storage

import com.github.ajalt.clikt.core.Abort
import com.phoenix.kstore.AbortTransactionException
import com.phoenix.kstore.utils.toByteArray
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.withLock
import java.nio.ByteBuffer
import java.util.*

enum class TransactionStatus {
    PENDING, COMMITTED, ABORTED, NOOP
}

class Transaction(private val store: Store) {

    val reads = hashSetOf<ByteArray>()
    val readTs = runBlocking { store.oracle.readTs() }
    val writes = linkedMapOf<ByteArray, Entry>()
    val isReadOnly: Boolean get() = writes.size == 0

    private var commitTs: Long? = null
    private val returning = hashMapOf<ByteArray, ByteArray?>()
    private val id = UUID.randomUUID().toString()
    private var status = TransactionStatus.PENDING

    /**
     * if this transaction has any writes for this key, return from there, else
     * load the latest version from its snapshot of the db and track the read key
     */
    fun read(key: ByteArray): ByteArray? {
        writes[key]?.also { return it.value }

        reads.add(key)

        val seek = encodeKeyWithTs(key, readTs)
        val version = store.read(seek)

        if (version == null) {
            returning.remove(key)
            return null
        }

        val (versionKey, _) = decodeKeyWithTs(version.key)
        if (!key.contentEquals(versionKey) || version.isDeleted()) {
            returning.remove(key)
            return null
        }

        returning[key] = version.value
        return version.value
    }

    fun write(key: ByteArray, value: ByteArray, meta: Int) {
        writes[key] = Entry(key, value, meta)
    }

    /**
     * Don't incur any overhead with oracle if no writes to process.
     * else, get a commit ts from oracle and apply to all writes then ship
     * over to db to persist
     */
    suspend fun commit(): Transaction {
        if (writes.size == 0) {
            status = TransactionStatus.NOOP
            return this
        }

        return store.oracle.writeMutex.withLock {
            commitTransaction()
        }
    }

    /**
     * @throws [AbortTransactionException]
     */
    private suspend fun commitTransaction(): Transaction {
        try {
            commitTs = store.oracle.commitRequest(this)
        } catch (err: AbortTransactionException) {
            status = TransactionStatus.ABORTED
            throw err
        }

        val toWrite = writes.map { (key, write) ->
            val keyWithTs = encodeKeyWithTs(key, commitTs!!)
            Entry(keyWithTs, write.value, write.meta)
        }

        store.write(toWrite)
        status = TransactionStatus.COMMITTED
        return this
    }

    private fun encodeKeyWithTs(key: ByteArray, ts: Long): ByteArray {
        return key + toByteArray(ts)
    }

    private fun decodeKeyWithTs(keyWithTs: ByteArray): Pair<ByteArray, Long> {
        val wrapped = ByteBuffer.wrap(keyWithTs)
        val tsStart = keyWithTs.size - Long.SIZE_BYTES
        val ts = wrapped.getLong(tsStart)
        return keyWithTs.copyOfRange(0, tsStart) to ts
    }
}
