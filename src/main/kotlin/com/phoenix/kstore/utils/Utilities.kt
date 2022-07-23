package com.phoenix.kstore.utils

import java.nio.ByteBuffer

fun toByteArray(vararg items: Int): ByteArray {
    val buffer = ByteBuffer.allocate(Int.SIZE_BYTES * items.size)
    items.forEach { buffer.putInt(it) }

    return buffer.array()
}

fun toByteArray(vararg items: Long): ByteArray {
    val buffer = ByteBuffer.allocate(Long.SIZE_BYTES * items.size)
    items.forEach { buffer.putLong(it) }

    return buffer.array()
}
