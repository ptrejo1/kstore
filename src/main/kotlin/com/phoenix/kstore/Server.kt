package com.phoenix.kstore

import com.phoenix.kstore.utils.Host
import java.util.*

class Server(
    val host: String = "127.0.0.1",
    val port: Int = 4000,
    val peerToPeerHost: String = "127.0.0.1",
    val peerToPeerPort: Int = 4001,
    val nodeName: String = UUID.randomUUID().toString()
) {

    val clientHost = Host(host, port)
    val p2pHost = Host(peerToPeerHost, peerToPeerPort)
    val node = Node(clientHost, p2pHost, nodeName)
}
