package com.phoenix.kstore.servers

import com.phoenix.kstore.grpc.*
import io.grpc.Server
import io.grpc.ServerBuilder

class PeerServer(private val port: Int) {

    val server: Server = ServerBuilder
        .forPort(port)
        .addService(PeerService())
        .build()

    fun start() {
        server.start()
        println("Server started, listening on $port")
        Runtime.getRuntime().addShutdownHook(
            Thread {
                println("*** shutting down gRPC server since JVM is shutting down")
                this@PeerServer.stop()
                println("*** server shut down")
            }
        )
    }

    private fun stop() {
        server.shutdown()
    }

    fun blockUntilShutdown() {
        server.awaitTermination()
    }

    private class PeerService : PeerServerGrpcKt.PeerServerCoroutineImplBase() {

        override suspend fun ping(request: Empty): Ack = Ack
            .newBuilder()
            .setAck(true)
            .build()

        override suspend fun pingRequest(request: PingReq): Ack = Ack
            .newBuilder()
            .setAck(true)
            .build()

        override suspend fun stateSync(request: State): State = State
            .newBuilder()
            .setReplicaId("")
            .setPeerAddress("")
            .putAllAddSet(mapOf())
            .putAllRemoveSet(mapOf())
            .build()
    }
}
