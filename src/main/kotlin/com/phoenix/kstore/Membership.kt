package com.phoenix.kstore

import com.phoenix.kstore.utils.NodeKey
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

class Membership(val nodeKey: NodeKey) {

    companion object {
        val FAILURE_DETECTION_INTERVAL = 0.5
        val FAILURE_DETECTION_SUBGROUP_SIZE = 3
        val GOSSIP_INTERVAL = 0.2
        val GOSSIP_SUBGROUP_SIZE = 5
        val JITTER = 0.01
    }

    val peers = HashMap<String, Peer>()
    val suspects = HashSet<String>()
    val suspectQueue = ConcurrentLinkedQueue<Peer>()
    val clusterState = LWWRegister(nodeKey.name)

    private val choices = HashSet<String>()
    private val mutex = Mutex()
    private val isStopped = false

    init {
        // TODO: Try avoid blocking
        runBlocking { clusterState.add(nodeKey.toString()) }

    }

    //** Initial state sync */
    fun bootstrap(joinNodeKey: NodeKey) {
        TODO()
    }

    private fun buildRouteTable() {

    }
}
