package com.phoenix.kstore

import com.phoenix.kstore.utils.Host
import com.phoenix.kstore.utils.NodeKeyRepr
import com.phoenix.kstore.utils.toNodeKey
import kotlinx.coroutines.*
import java.util.*

class Server(
    val hostname: String = "127.0.0.1",
    val port: Int = 4000,
    val peerToPeerHostname: String = "127.0.0.1",
    val peerToPeerPort: Int = 4001,
    val nodeName: String = UUID.randomUUID().toString(),
    val joinNodeKeyRepr: NodeKeyRepr? = null
) {

    private val clientHost = Host(hostname, port)
    private val p2pHost = Host(peerToPeerHostname, peerToPeerPort)
    private val node = Node(clientHost, p2pHost, nodeName)
    private val peerServer = PeerServer(peerToPeerPort, node)
    private val jobsScope = CoroutineScope(Dispatchers.Default)
    private val jobs: List<Job>

    init {
        jobs = listOf(
            jobsScope.launch(Dispatchers.Default, CoroutineStart.LAZY) {
                startMembership()
            },
            jobsScope.launch(Dispatchers.Default, CoroutineStart.LAZY) {
                startPeerServer()
            }
        )
    }

    suspend fun start() {
        Runtime.getRuntime().addShutdownHook(Thread {
            stop()
        })

        if (joinNodeKeyRepr != null)
            node.bootstrap(joinNodeKeyRepr.toNodeKey())

        jobs.forEach { it.join() }
    }

    private fun stop() {
        node.membership.stop()
        peerServer.stop()
    }

    private fun startMembership() = node.membership.start()

    private fun startPeerServer() = peerServer.start()
}
