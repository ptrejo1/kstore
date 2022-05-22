package com.phoenix.kstore.nodes

import com.phoenix.kstore.Server
import kotlinx.coroutines.runBlocking

fun main() {
    val server = Server(
        hostname =  "127.0.0.1",
        port = 4000,
        peerToPeerHostname = "127.0.0.1",
        peerToPeerPort = 4001,
        nodeName = "uno"
    )
    runBlocking { server.start() }
}
