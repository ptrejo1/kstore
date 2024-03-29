package com.phoenix.kstore.storage

import java.util.*
import kotlin.math.max

class Node(var key: IndexEntry) {

    var left: Node? = null
    var right: Node? = null
    var maximum: Node? = this
    var minimum: Node? = this
    var height: Int = 1
}

enum class ComparisonType {
    LTE, EQ, GTE
}


class AVLTree {

    var root: Node? = null

    /**
     * BST search. if [comparisonType] is GTE, find exact match or closest node gte search key
     * or if [comparisonType] is LTE, find exact match or closest node lte search key
     * else EQ look for exact match.
     */
    fun search(key: ByteArray, comparisonType: ComparisonType = ComparisonType.EQ): IndexEntry? =
        search(root, IndexEntry(key, 0), comparisonType)

    fun insert(key: IndexEntry) {
        val node = Node(key)
        root = insert(root, node)
    }

    private fun search(root: Node?, key: IndexEntry, comparisonType: ComparisonType): IndexEntry? {
        if (root == null) return null

        val cmp = compare(key, root.key)
        if (cmp < 0) {
            if (comparisonType == ComparisonType.GTE) {
                val check = (
                    root.left == null ||
                    (compare(key, root.left!!.key) > 0
                    && root.left!!.maximum != null
                    && compare(key, root.left!!.maximum!!.key) > 0)
                )
                if (check) return root.key
            }

            return search(root.left, key, comparisonType)
        } else if ( cmp > 0) {
            if (comparisonType == ComparisonType.LTE) {
                val check = (
                    root.right == null ||
                    (compare(key, root.right!!.key) < 0
                    && root.right!!.minimum != null
                    && compare(key, root.right!!.minimum!!.key) < 0)
                )
                if (check) return root.key
            }

            return search(root.right, key, comparisonType)
        }

        return root.key
    }

    private fun compare(first: IndexEntry, other: IndexEntry): Int =
        Arrays.compare(first.key, other.key)

    private fun insert(root: Node?, node: Node): Node {
        if (root == null) return node

        val cmp = compare(node.key, root.key)
        if (cmp == 0) {
            root.key = node.key
        } else if (cmp < 0) {
            root.minimum = node
            root.left = insert(root.left, node)
        } else {
            root.maximum = node
            root.right = insert(root.right, node)
        }

        val leftHeight = getHeight(root.left)
        val rightHeight = getHeight(root.right)
        root.height = 1 + max(leftHeight, rightHeight)
        val balance = leftHeight - rightHeight
        var result = root

        if (balance > 1 && root.left != null && compare(node.key, root.left!!.key) < 0) {
            result = rightRotate(root)
        } else if (balance < -1 && root.right != null && compare(node.key, root.right!!.key) > 0) {
            result = leftRotate(root)
        } else if (balance > 1 && root.left != null && compare(node.key, root.left!!.key) > 0) {
            root.left = leftRotate(root.left!!)
            result = rightRotate(root)
        } else if (balance < -1 && root.right != null && compare(node.key, root.right!!.key) < 0) {
            root.right = rightRotate(root.right!!)
            result = leftRotate(root)
        }

        return result
    }

    private fun getHeight(node: Node?): Int = node?.height ?: 0

    private fun leftRotate(node: Node): Node {
        val right = node.right ?: return node
        val rLeft = right.left
        right.left = node
        node.right = rLeft
        node.height = 1 + max(getHeight(node.left), getHeight(node.right))
        right.height = 1 + max(getHeight(right.left), getHeight(right.right))

        return right
    }

    private fun rightRotate(node: Node): Node {
        val left = node.left ?: return node
        val lRight = left.right
        left.right = node
        node.left = lRight
        node.height = 1 + max(getHeight(node.left), getHeight(node.right))
        left.height = 1 + max(getHeight(left.left), getHeight(left.right))

        return left
    }
}
