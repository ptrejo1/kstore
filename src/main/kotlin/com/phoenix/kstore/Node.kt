package com.phoenix.kstore

import com.phoenix.kstore.storage.EntryMeta
import com.phoenix.kstore.storage.Store
import com.phoenix.kstore.storage.Transaction
import com.phoenix.kstore.utils.Host
import com.phoenix.kstore.utils.NodeKey
import com.phoenix.kstore.utils.getLogger
import com.phoenix.kstore.utils.toByteArray

class Node(
    val clientHost: Host,
    val p2pHost: Host,
    val name: String,
) {

    companion object {
        private val logger by getLogger()
    }

    private val nodeKey = NodeKey(name, p2pHost)
    private val store = Store()
    val membership: Membership = Membership(nodeKey)
    val router = Router(membership, this)

    suspend fun bootstrap(joinNodeKey: NodeKey) {
        membership.bootstrap(joinNodeKey)
        logger.info("Bootstrapping Node - joining: $joinNodeKey")
    }

    /**
     * Handle request this node is responsible for
     */
    suspend fun coordinate(request: BatchRequest): Transaction {
        logger.info("start ${request.table()} ${request.requests}")
        val transaction = store.withTransaction { txn ->
            request.requests.forEach {
                when (it) {
                    is GetRequest -> txn.read(it.key)
                    is PutRequest -> txn.write(it.key, it.value)
                    is DeleteRequest -> txn.write(it.key, byteArrayOf(), EntryMeta.TOMBSTONED.value)
                }
            }

            txn
        }

        logger.info("done ${request.table()} ${transaction.returning}")
        return transaction
    }
}
