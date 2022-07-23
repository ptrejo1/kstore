package com.phoenix.kstore.storage

import com.phoenix.kstore.ChecksumValidationException
import com.phoenix.kstore.utils.toByteArray
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.zip.CRC32

enum class EntryMeta(val value: Int) {
    ALIVE(0), TOMBSTONED(1)
}

class Entry(val key: ByteArray, val value: ByteArray, val meta: Int) {

    companion object {

        /**
         * @throws [ChecksumValidationException]
         */
        fun decode(buffer: ByteArray): Entry {
            val wrapped = ByteBuffer.wrap(buffer)
            val decodedMeta = wrapped.getInt(4)
            val decodedKeyLength = wrapped.getInt(8)
            val decodedValueLength = wrapped.getInt(12)
            val decodedKey = buffer.copyOfRange(16, 16 + decodedKeyLength)
            val valueStart = 16 + decodedKeyLength
            val valueEnd = valueStart + decodedValueLength
            val decodedValue = buffer.copyOfRange(valueStart, valueEnd)
            val checksum = wrapped.getLong(valueEnd)

            val crc = CRC32()
            val header = toByteArray(decodedMeta, decodedKey.size, decodedValue.size)
            crc.update(header)
            crc.update(decodedKey)
            crc.update(decodedValue)

            if (checksum != crc.value)
                throw ChecksumValidationException()

            return Entry(decodedKey, decodedValue, decodedMeta)
        }
    }

    fun isDeleted(): Boolean = meta == EntryMeta.TOMBSTONED.value

    /**
     * byte array representation of log entry.
     * append CRC32 checksum of header and k/v
     * -----------------------------------------------------------------------
     * | block size (Int) | header | key | value | crc32 (Long) |
     *                        where header is
     * | meta (Int) | key length (Int) | value length (Int) |
     * -----------------------------------------------------------------------
     * TODO: Add compression
     */
    fun encode(): ByteArray {
        val crc = CRC32()
        val data = ByteArrayOutputStream()

        val header = toByteArray(meta, key.size, value.size)
        crc.update(header)
        data.write(header)

        crc.update(key)
        data.write(key)

        crc.update(value)
        data.write(value)

        val checksum = toByteArray(crc.value)
        data.write(checksum)

        val encoded = ByteArrayOutputStream()
        encoded.write(toByteArray(data.size()))
        encoded.write(data.toByteArray())

        return encoded.toByteArray()
    }
}
