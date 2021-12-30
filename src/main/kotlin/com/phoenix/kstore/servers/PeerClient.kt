package com.phoenix.kstore.servers

import com.phoenix.kstore.PackedHLCTimestamp
import com.phoenix.kstore.grpc.*
import com.phoenix.kstore.toSetElements
import com.phoenix.kstore.utils.Host
import com.phoenix.kstore.utils.NodeKey
import com.phoenix.kstore.utils.NodeKeyRepr
import com.phoenix.kstore.utils.NodeName
import io.grpc.ManagedChannel
import java.io.Closeable
import java.util.concurrent.TimeUnit

/** Client for communicating with [PeerServer] */
class PeerClient(private val channel: ManagedChannel) : Closeable {

    private val stub: PeerServerGrpcKt.PeerServerCoroutineStub =
        PeerServerGrpcKt.PeerServerCoroutineStub(channel)

    suspend fun ping(): Ack {
        val empty = Empty.newBuilder().build()
        return stub.ping(empty)
    }

    suspend fun pingRequest(nodeKey: NodeKey): Ack {
        val pingReq = PingReq.newBuilder()
            .setPeerName(nodeKey.name)
            .setPeerHost(nodeKey.host.toString())
            .build()

        return stub.pingRequest(pingReq)
    }

    suspend fun stateSync(
        nodeName: NodeName,
        host: Host,
        addSet: LinkedHashMap<NodeKeyRepr, PackedHLCTimestamp>,
        removeSet: LinkedHashMap<NodeKeyRepr, PackedHLCTimestamp>
    ): State {
        val state = State.newBuilder()
            .setNodeName(nodeName)
            .setPeerHost(host.toString())
            .addAllAddSet(addSet.toSetElements())
            .addAllRemoveSet(removeSet.toSetElements())
            .build()

        return stub.stateSync(state)
    }

    override fun close() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }
}
