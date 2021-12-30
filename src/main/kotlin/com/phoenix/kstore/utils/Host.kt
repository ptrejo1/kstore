package com.phoenix.kstore.utils

class Host {

    val hostname: String
    val port: Int

    constructor(hostname: String, port: Int) {
        this.hostname = hostname
        this.port = port
    }

    constructor(host: String) {
        val hostTokens = host.split(":")
        this.hostname = hostTokens[0]
        this.port = hostTokens[1].toInt()
    }

    override fun toString(): String {
        return "$hostname:$port"
    }
}
