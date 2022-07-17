package com.phoenix.kstore.storage

import com.phoenix.kstore.TableOverflowException
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

/**
 * In memory repr of store
 */
class MemTable(private val maxSize: Int) {

    val size: Int get() = arena.size()

    private val arena = ByteArrayOutputStream()
    private val index = AVLTree()

    private var entriesCount = 0
    private var offset = 0

    fun put(entry: Entry) {
        val encoded = entry.encode()
        if (arena.size() + encoded.size > maxSize)
            throw TableOverflowException()

        index.insert(IndexEntry(entry.key, offset))
        arena.write(encoded)
        entriesCount += 1
        offset += encoded.size
    }

    fun get(key: ByteArray): Entry? {
        val indexEntry = index.search(key, true) ?: return null
        val (entry, _) = decodeAtOffset(indexEntry.offset)

        return entry
    }

    fun scan() = sequence {
        var runningOffset = 0
        while (runningOffset < arena.size()) {
            val (entry, bytesRead) = decodeAtOffset(runningOffset)
            yield(entry)
            runningOffset += bytesRead
        }
    }

    private fun decodeAtOffset(offset: Int): Pair<Entry, Int> {
        val arenaArray = arena.toByteArray()
        val buffer = ByteBuffer.wrap(arenaArray)
        val blockSize = buffer.getInt(offset)

        // offset + size of blockSize + size of block
        val blockEnd = offset + Int.SIZE_BYTES + blockSize
        val chunk = arenaArray.copyOfRange(offset, blockEnd)
        val bytesRead = blockEnd - offset

        return Entry.decode(chunk) to bytesRead
    }
}
