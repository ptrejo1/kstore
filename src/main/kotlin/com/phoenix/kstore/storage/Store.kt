package com.phoenix.kstore.storage

class Store(private val maxTableSize: Int = 1024 shl 20) {

    val oracle = Oracle()
    val memTable = MemTable(maxTableSize)

    fun read(key: ByteArray): Entry? = memTable.get(key)

    fun write(entries: List<Entry>) {
        entries.forEach { memTable.put(it) }
    }
}
