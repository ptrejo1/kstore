package com.phoenix.kstore

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

    /** @throws Exception if fails to reach peer */
    suspend fun ping(): Boolean = peerClient.ping().ack

    /** @throws Exception if fails to reach peer */
    suspend fun pingRequest(peer: Peer): Boolean =
        peerClient.pingRequest(peer.nodeKey).ack

    /** @throws Exception if fails to reach peer */
    suspend fun stateSync(state: LWWRegister, fromHost: Host): LWWRegister {
        val incomingState = peerClient.stateSync(
            state.nodeName,
            fromHost,
            state.addSet,
            state.removeSet
        )
        val register = LWWRegister(incomingState.nodeName)
        register.addSet = incomingState.addSetList.toRegisterSet()
        register.removeSet = incomingState.removeSetList.toRegisterSet()

        return register
    }
}
