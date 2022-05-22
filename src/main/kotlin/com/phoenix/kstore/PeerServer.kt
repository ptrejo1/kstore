package com.phoenix.kstore

import com.phoenix.kstore.grpc.*
import com.phoenix.kstore.utils.Host
import com.phoenix.kstore.utils.NodeKey
import com.phoenix.kstore.utils.getLogger
import io.grpc.Server
import io.grpc.ServerBuilder
import java.util.concurrent.TimeUnit

/** Server for p2p communication */
class PeerServer(port: Int, node: Node) {

    companion object {
        private val logger by getLogger()
    }

    val server: Server = ServerBuilder
        .forPort(port)
        .addService(PeerService(node))
        .build()

    fun start() {
        server.start()
        logger.info("PeerServer Started")
        server.awaitTermination()
    }

    fun stop() {
        logger.info("PeerServer shutting down...")
        server.shutdown().awaitTermination(10, TimeUnit.SECONDS)
        logger.info("PeerServer shut down")
    }
}

private class PeerService(private val node: Node): PeerServerGrpcKt.PeerServerCoroutineImplBase() {

    override suspend fun ping(request: Empty): Ack = Ack
        .newBuilder()
        .setAck(true)
        .build()

    override suspend fun pingRequest(request: PingReq): Ack {
        val nodeKey = NodeKey(request.peerName, request.peerHost)
        val ack = node.membership.pingRequest(nodeKey)

        return Ack
            .newBuilder()
            .setAck(ack)
            .build()
    }

    override suspend fun stateSync(request: State): State {
        val incoming = LWWRegister(request.nodeName)
        incoming.addSet = request.addSetList.toRegisterSet()
        incoming.removeSet = request.removeSetList.toRegisterSet()
        val state = node.membership.stateSync(incoming, Host(request.peerHost))

        return State
            .newBuilder()
            .setNodeName(node.name)
            .setPeerHost(node.p2pHost.toString())
            .addAllAddSet(state.addSet.toSetElements())
            .addAllRemoveSet(state.removeSet.toSetElements())
            .build()
    }
}
