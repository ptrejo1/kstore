package com.phoenix.kstore

import com.phoenix.kstore.utils.Host
import com.phoenix.kstore.utils.NodeKey
import com.phoenix.kstore.utils.getLogger

class Node(
    val clientHost: Host,
    val p2pHost: Host,
    val name: String,
) {

    companion object {
        private val logger by getLogger()
    }

    private val nodeKey = NodeKey(name, p2pHost)
    val membership: Membership = Membership(nodeKey)

    suspend fun bootstrap(joinNodeKey: NodeKey) {
        membership.bootstrap(joinNodeKey)
        logger.info("Bootstrapping Node - joining: $joinNodeKey")
    }
}
