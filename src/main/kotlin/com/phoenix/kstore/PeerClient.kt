package com.phoenix.kstore

import com.google.protobuf.kotlin.toByteString
import com.phoenix.kstore.grpc.*
import com.phoenix.kstore.grpc.BatchResponse
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
        val empty = Empty.newBuilder()
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

    suspend fun coordinate(request: BatchRequest): Result<BatchResponse> {
        val requests = request.requests.map { req ->
            when (req) {
                is GetRequest -> {
                    val get = com.phoenix.kstore.grpc.GetRequest.newBuilder()
                        .setKey(req.key.toByteString())
                        .build()
                    com.phoenix.kstore.grpc.Request.newBuilder()
                        .setGet(get)
                        .build()
                }
                is PutRequest -> {
                    val put = com.phoenix.kstore.grpc.PutRequest.newBuilder()
                        .setKey(req.key.toByteString())
                        .setValue(req.value.toByteString())
                        .build()
                    com.phoenix.kstore.grpc.Request.newBuilder()
                        .setPut(put)
                        .build()
                }
                else -> {
                    val delete = com.phoenix.kstore.grpc.DeleteRequest.newBuilder()
                        .setKey(req.key.toByteString())
                        .build()
                    com.phoenix.kstore.grpc.Request.newBuilder()
                        .setDelete(delete)
                        .build()
                }
            }
        }

        val msg = com.phoenix.kstore.grpc.BatchRequest.newBuilder()
            .setTable(request.table())
            .addAllRequests(requests)
            .build()

        return execute { stub.coordinate(msg) }
    }

    override fun close() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }
}
