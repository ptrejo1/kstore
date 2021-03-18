package com.phoenix.kstore.utils

data class Host(val hostname: String, val port: Int) {

    override fun toString(): String {
        return "$hostname:$port"
    }
}
