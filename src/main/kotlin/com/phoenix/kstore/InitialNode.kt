package com.phoenix.kstore

import kotlinx.coroutines.runBlocking

fun main() {
    val server = Server(nodeName = "phoenix")
    runBlocking { server.start() }
}
