package com.phoenix.kstore

import kotlinx.coroutines.runBlocking

fun main() {
    val server = Server(
        hostname =  "127.0.0.1",
        port = 4004,
        peerToPeerHostname = "127.0.0.1",
        peerToPeerPort = 4005,
        nodeName = "tres",
        joinNodeKeyRepr = "dos=127.0.0.1:4003"
    )
    runBlocking { server.start() }
}
