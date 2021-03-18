package com.phoenix.kstore

import com.phoenix.kstore.servers.PeerClient
import com.phoenix.kstore.utils.Host
import com.phoenix.kstore.utils.NodeKey
import io.grpc.ManagedChannelBuilder
import java.util.*

class Peer(private val nodeKey: NodeKey) {

    private val channel = ManagedChannelBuilder
        .forTarget(nodeKey.host.toString())
        .usePlaintext()
        .build()
    private val peerClient = PeerClient(channel)

    suspend fun ping(): Boolean {
        return peerClient.ping().ack
    }

    suspend fun pingReq(peer: Peer): Boolean {
        return peerClient.pingRequest(peer.nodeKey.name, peer.nodeKey.host.toString()).ack
    }

    suspend fun stateSync(state: LWWRegister, fromHost: Host): LWWRegister {
        val incomingState =  peerClient.stateSync(state.replicaId, fromHost, state.addSet, state.removeSet)
        val register = LWWRegister(incomingState.replicaId)
        register.addSet = incomingState.addSetMap as LinkedHashMap<String, PackedHLCTimestamp>
        register.removeSet = incomingState.removeSetMap as LinkedHashMap<String, PackedHLCTimestamp>

        return register
    }
}
