package com.phoenix.kstore.servers

import com.phoenix.kstore.grpc.*
import com.phoenix.kstore.utils.Host
import io.grpc.ManagedChannel
import java.io.Closeable
import java.util.concurrent.TimeUnit

class PeerClient(private val channel: ManagedChannel) : Closeable {

    private val stub: PeerServerGrpcKt.PeerServerCoroutineStub = PeerServerGrpcKt.PeerServerCoroutineStub(channel)

    suspend fun ping(): Ack {
        val empty = Empty.newBuilder().build()
        return stub.ping(empty)
    }

    suspend fun pingRequest(peerName: String, peerAddress: String): Ack {
        val pingReq = PingReq.newBuilder()
            .setPeerName(peerName)
            .setPeerAddress(peerAddress)
            .build()
        return stub.pingRequest(pingReq)
    }

    suspend fun stateSync(
        replicaId: String,
        host: Host,
        addSet: Map<String, Long>,
        removeSet: Map<String, Long>
    ): State {
        val state = State.newBuilder()
            .setReplicaId(replicaId)
            .setPeerAddress(host.toString())
            .putAllAddSet(addSet)
            .putAllRemoveSet(removeSet)
            .build()
        return stub.stateSync(state)
    }

    override fun close() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }
}
