package com.phoenix.kstore

import com.phoenix.kstore.grpc.Ack
import com.phoenix.kstore.servers.PeerClient
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

    override fun toString(): String {
        return nodeKey.toString()
    }
}
