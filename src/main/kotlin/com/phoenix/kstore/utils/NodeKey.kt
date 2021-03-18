package com.phoenix.kstore.utils

data class NodeKey(val name: String, val host: Host) {

    override fun toString(): String {
        return "$name=$host"
    }
}
