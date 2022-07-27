package com.phoenix.kstore

import com.phoenix.kstore.grpc.Ack
import com.phoenix.kstore.storage.TransactionInfo
import com.phoenix.kstore.storage.TransactionStatus
import com.phoenix.kstore.utils.Host
import com.phoenix.kstore.utils.NodeKey
import io.grpc.ManagedChannelBuilder

class Peer(val nodeKey: NodeKey) {

    private val channel = ManagedChannelBuilder
        .forTarget(nodeKey.host.toString())
        .usePlaintext()
        .build()
    private val peerClient = PeerClient(channel)

    val name = nodeKey.name

    suspend fun ping(): Result<Ack> = peerClient.ping()

    suspend fun pingRequest(peer: Peer): Result<Ack> =
        peerClient.pingRequest(peer.nodeKey)

    suspend fun stateSync(state: LWWRegister, fromHost: Host): Result<LWWRegister> {
        val result = peerClient.stateSync(
            state.nodeName,
            fromHost,
            state.addSet,
            state.removeSet
        )
        val incomingState = result.getOrElse { return Result.failure(it) }

        val register = LWWRegister(incomingState.nodeName)
        register.addSet = incomingState.addSetList.toRegisterSet()
        register.removeSet = incomingState.removeSetList.toRegisterSet()

        return Result.success(register)
    }

    suspend fun coordinate(request: BatchRequest): Result<BatchResponse> {
        val response = peerClient.coordinate(request).getOrElse {
            return Result.failure(it)
        }
        val info = TransactionInfo(
            response.txn.txnId,
            response.txn.readTs,
            response.txn.commitTs,
            TransactionStatus.valueOf(response.txn.status.name),
            HashMap(response.txn.returningMap.entries
                .associate { it.key.toByteArray() to it.value.toByteArray() }
            )
        )

        return Result.success(BatchResponse(response.table, info))
    }

    override fun toString(): String {
        return nodeKey.toString()
    }
}
