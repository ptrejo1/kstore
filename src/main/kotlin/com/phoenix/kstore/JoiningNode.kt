package com.phoenix.kstore

import kotlinx.coroutines.runBlocking

fun main() {
    val server = Server(
        port = 4002,
        peerToPeerPort = 4003,
        nodeName = "arizona",
        joinNodeKey = "phoenix=127.0.0.1:4001"
    )
    runBlocking { server.start() }
}
