package com.phoenix.kstore.storage

import kotlinx.coroutines.runBlocking
import java.util.UUID

enum class TransactionStatus {
    PENDING, COMMITTED, ABORTED, NOOP
}

// TODO: Come back after MemTable added to Store
class Transaction(private val store: Store) {

    /** fix with right type */
    val reads = hashSetOf<String>()
    val readTs = runBlocking { store.oracle.readTs() }
    /** fix with right type */
    val writes = linkedMapOf<String, String>()

    /** fix with right type */
    private val returning = hashMapOf<String, String?>()
    private val uuid = UUID.randomUUID().toString()
    private val commitTs: Long? = null
    private val status = TransactionStatus.PENDING
}
