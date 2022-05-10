package com.phoenix.kstore.servers

import com.phoenix.kstore.PackedHLCTimestamp
import com.phoenix.kstore.grpc.*
import com.phoenix.kstore.toSetElements
import com.phoenix.kstore.utils.*
import io.grpc.ManagedChannel
import io.grpc.StatusException
import java.io.Closeable
import java.util.concurrent.TimeUnit

/** Client for communicating with [PeerServer] */
class PeerClient(private val channel: ManagedChannel) : Closeable {

    companion object {
        private val logger by getLogger()
    }

    private suspend fun <T> execute(block: suspend () -> T): Result<T> {
        return try {
            Result.success(block())
        } catch (e: StatusException) {
            logger.error(e.stackTraceToString())
            Result.failure(e)
        }
    }

    private val stub: PeerServerGrpcKt.PeerServerCoroutineStub =
        PeerServerGrpcKt.PeerServerCoroutineStub(channel)

    suspend fun ping(): Result<Ack> {
        val empty = Empty
            .newBuilder()
            .build()

        return execute { stub.ping(empty) }
    }

    suspend fun pingRequest(nodeKey: NodeKey): Result<Ack> {
        val pingReq = PingReq.newBuilder()
            .setPeerName(nodeKey.name)
            .setPeerHost(nodeKey.host.toString())
            .build()

        return execute { stub.pingRequest(pingReq) }
    }

    suspend fun stateSync(
        nodeName: NodeName,
        host: Host,
        addSet: LinkedHashMap<NodeKeyRepr, PackedHLCTimestamp>,
        removeSet: LinkedHashMap<NodeKeyRepr, PackedHLCTimestamp>
    ): Result<State> {
        val state = State.newBuilder()
            .setNodeName(nodeName)
            .setPeerHost(host.toString())
            .addAllAddSet(addSet.toSetElements())
            .addAllRemoveSet(removeSet.toSetElements())
            .build()

        return execute { stub.stateSync(state) }
    }

    override fun close() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }
}
