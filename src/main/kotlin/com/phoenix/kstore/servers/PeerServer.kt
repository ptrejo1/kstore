package com.phoenix.kstore.servers

import com.phoenix.kstore.LWWRegister
import com.phoenix.kstore.Node
import com.phoenix.kstore.grpc.*
import com.phoenix.kstore.toRegisterSet
import com.phoenix.kstore.toSetElements
import com.phoenix.kstore.utils.Host
import com.phoenix.kstore.utils.NodeKey
import io.grpc.Server
import io.grpc.ServerBuilder
import java.util.concurrent.TimeUnit

/** Server for p2p communication */
class PeerServer(private val port: Int, node: Node) {

    val server: Server = ServerBuilder
        .forPort(port)
        .addService(PeerService(node))
        .build()

    fun start() {
        server.start()
        println("Server started, listening on $port")
        server.awaitTermination()
    }

    fun stop() {
        println("*** shutting down gRPC server")
        server.shutdown().awaitTermination(10, TimeUnit.SECONDS)
        println("*** server shut down")
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
