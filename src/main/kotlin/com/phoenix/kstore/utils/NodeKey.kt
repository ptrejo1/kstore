package com.phoenix.kstore.utils

/**
 * Name of node, usually a uuid
 */
typealias NodeName = String

/**
 * String representation of [NodeKey]
 */
typealias NodeKeyRepr = String

fun NodeKeyRepr.toNodeKey(): NodeKey {
    val tokens =  this.split("=")
    val nodeName = tokens[0]
    val host = tokens[1]
    val hostTokens = host.split(":")
    val hostname = hostTokens[0]
    val port = hostTokens[1].toInt()

    return NodeKey(nodeName, Host(hostname, port))
}

/**
 * Composed of [NodeName] and [Host]
 */
data class NodeKey(val name: NodeName, val host: Host) {

    override fun toString(): NodeKeyRepr {
        return "$name=$host"
    }
}
