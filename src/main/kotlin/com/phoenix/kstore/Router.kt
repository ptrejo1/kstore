package com.phoenix.kstore

import com.phoenix.kstore.storage.TransactionInfo
import com.phoenix.kstore.utils.getLogger

interface Request {
    val key: ByteArray
}

class GetRequest(override val key: ByteArray): Request

class PutRequest(override val key: ByteArray, val value: ByteArray): Request

class DeleteRequest(override val key: ByteArray): Request

class BatchRequest(val requests: List<Request>) {

    fun table(): String {
        if (requests.isEmpty())
            throw InvalidRequestException("no requests")

        // match /table/pkey
        val requestKeyRegex = """^/([A-Za-z\d]+)/([A-Za-z\d]+)${'$'}""".toRegex()
        val g = requestKeyRegex.matchEntire(requests.first().key.toString(Charsets.UTF_8))
            ?: throw InvalidRequestException("invalid key")

        return g.groups[1]!!.value
    }
}

class BatchResponse(val table: String, val transactionInfo: TransactionInfo)

class Router(private val membership: Membership, val node: Node) {

    companion object {
        val logger by getLogger()
    }

    suspend fun request(request: BatchRequest): BatchResponse? {
        membership.lookupLeaseHolder(request.table())?.also { peer ->
            logger.info("remote $peer ${request.table()}")
            return peer.coordinate(request).getOrNull()
        }

        logger.info("local ${request.table()}")
        val txn = node.coordinate(request)
        val info = TransactionInfo(
            txn.id,
            txn.readTs,
            txn.commitTs,
            txn.status,
            txn.returning
        )
        return BatchResponse(request.table(), info)
    }
}
