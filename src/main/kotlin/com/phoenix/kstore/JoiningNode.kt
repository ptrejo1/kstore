package com.phoenix.kstore

import kotlinx.coroutines.runBlocking

fun main() {
    val server = Server(
        hostname =  "127.0.0.1",
        port = 4002,
        peerToPeerHostname = "127.0.0.1",
        peerToPeerPort = 4003,
        nodeName = "dos",
        joinNodeKeyRepr = "uno=127.0.0.1:4001"
    )
    runBlocking { server.start() }
}
