package com.phoenix.kstore

import com.phoenix.kstore.utils.NodeName
import net.openhft.hashing.LongHashFunction.xx
import kotlin.math.abs

/**
 * Maglev hashing implementation
 * More info here: https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/44824.pdf
 */
class Maglev(nodes: HashSet<NodeName>) {

    companion object {

        private const val MAGLEV_OFFSET_SEED = 0xdeadbabe
        private const val MAGLEV_SKIP_SEED = 0xdeadbeef
    }

    private val nodes: List<NodeName> = nodes.toList()
    private val n = this.nodes.count()
    private val m = nextPrime(n * 100)
    private val table = populate()

    /**
     * Get node for key
     */
    fun lookup(key: NodeName): NodeName {
        val hashed = abs(xx(MAGLEV_OFFSET_SEED).hashChars(key))
        return nodes[table[(hashed % m).toInt()]]
    }

    private fun generatePermutation(): List<List<Long>> {
        val permutation = mutableListOf<MutableList<Long>>()

        for ((i, name) in nodes.withIndex()) {
            val offset = abs(xx(MAGLEV_OFFSET_SEED).hashChars(name)) % m
            val skip = abs(xx(MAGLEV_SKIP_SEED).hashChars(name)) % (m - 1) + 1

            permutation.add(mutableListOf())
            for (j in 0 until m)
                permutation[i].add((offset + j * skip) % m)
        }

        return permutation
    }

    private fun populate(): List<Int> {
        if (n == 0) return listOf()

        val permutation = generatePermutation()
        val next = Array(n) { 0 }
        val entry = Array(m) { -1 }
        var k = 0

        while (true) {
            for (i in 0 until n) {
                var c = permutation[i][next[i]]

                while (entry[c.toInt()] >= 0) {
                    next[i] += 1
                    c = permutation[i][next[i]]
                }

                entry[c.toInt()] = i
                next[i] += 1
                k += 1

                if (k == m)
                    return entry.asList()
            }
        }
    }

    private fun isPrime(n: Int): Boolean {
        if (n <= 1) return false
        if (n <= 3) return true

        if (n % 2 == 0 || n % 3 == 0) return false

        var i = 5
        while (i * i <= n) {
            if (n % i == 0 || n % (i + 2) == 0)
                return false
            i += 6
        }

        return true
    }

    private fun nextPrime(n: Int): Int {
        if (n <= 1) return 2

        var prime = n
        var found = false

        while (!found) {
            prime++
            if (isPrime(prime))
                found = true
        }

        return prime
    }
}
