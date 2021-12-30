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
    val host = Host(tokens[1])

    return NodeKey(nodeName, host)
}

/**
 * Composed of [NodeName] and [Host]
 */
class NodeKey {

    val name: NodeName
    val host: Host

    constructor(name: NodeName, host: Host) {
        this.name = name
        this.host = host
    }

    constructor(name: NodeName, host: String) {
        this.name = name
        this.host = Host(host)
    }

    override fun toString(): NodeKeyRepr {
        return "$name=$host"
    }
}
