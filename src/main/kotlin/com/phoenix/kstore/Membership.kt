package com.phoenix.kstore

import com.phoenix.kstore.utils.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.random.Random

class Membership(val nodeKey: NodeKey) {

    companion object {
        const val FAILURE_DETECTION_INTERVAL_MS = 500L
        const val FAILURE_DETECTION_SUBGROUP_SIZE = 3
        const val GOSSIP_INTERVAL_MS = 200L
        const val GOSSIP_SUBGROUP_SIZE = 5
        const val JITTER_MS = 10L
        const val STARTUP_GRACE_PERIOD_SECONDS = 2

        val logger by getLogger()
    }

    val peers = HashMap<NodeName, Peer>()
    val suspects = HashSet<NodeKeyRepr>()
    val suspectQueue = ConcurrentLinkedQueue<Peer>()
    val clusterState = LWWRegister(nodeKey.name)

    private var choices = HashSet<NodeKeyRepr>()
    private val mutex = Mutex()
    private var isStopped = false
    private var maglev = Maglev(hashSetOf())
    private var jobs = listOf<Job>()
    private val jobsScope = CoroutineScope(Dispatchers.Default)

    init {
        // TODO: Try avoid blocking
        runBlocking { clusterState.add(nodeKey) }
        buildRouteTable()
    }

    /** Initial state sync */
    suspend fun bootstrap(peerNodeKey: NodeKey) {
        val peer = addPeer(peerNodeKey)
        syncWithPeer(peer)
    }

    suspend fun start() {
        logger.info("membership.start")
        jobs = listOf(
            jobsScope.launch(Dispatchers.Default) {
                failureDetectionLoop()
            },
            jobsScope.launch(Dispatchers.Default) {
                gossipLoop()
            },
            jobsScope.launch(Dispatchers.Default) {
                investigationLoop()
            }
        )
    }

    suspend fun stop() {
        isStopped = true
        jobs.forEach { it.cancel() }
        logger.info("membership.stop")
    }

    private suspend fun getAddPeer(nodeName: NodeName, host: Host): Peer {
        if (nodeName in peers) return peers[nodeName]!!
        return addPeer(NodeKey(nodeName, host))
    }

    private fun buildRouteTable() {
        // TODO: Avoid splitting on =
        val nodeNames = clusterState.state
            .map { it.first.toNodeKey().name }
        maglev = Maglev(nodeNames.toHashSet())
    }

    private suspend fun addPeer(peerNodeKey: NodeKey): Peer = mutex.withLock {
        val peer = Peer(peerNodeKey)
        peers[peerNodeKey.name] = peer
        clusterState.add(peer.nodeKey)
        buildRouteTable()

        peer
    }

    private suspend fun syncWithPeer(peer: Peer): LWWRegister {
        val peerMergedState = peer.stateSync(clusterState, nodeKey.host)
        return stateSync(peerMergedState, peer.nodeKey.host)
    }

    private suspend fun stateSync(incoming: LWWRegister, host: Host): LWWRegister {
        getAddPeer(incoming.nodeName, host)

        mutex.withLock {
            clusterState.merge(incoming)
            buildRouteTable()
        }

        return clusterState
    }

    private suspend fun failureDetectionLoop() {
        while (!isStopped) {
            probeRandomPeer()
            delayWithInterval(FAILURE_DETECTION_INTERVAL_MS)
        }
    }

    private suspend fun probeRandomPeer() {
        val peer = getRandomPeer() ?: return
        logger.info("membership.probe ${peer.nodeKey}")

        failureDetection(peer) { peer.ping() }
    }

    private suspend fun getRandomPeer(): Peer? {
        val eligiblePeers = eligiblePeers()

        if (choices.size >= eligiblePeers.size) choices = hashSetOf()

        val filtered = eligiblePeers
            .filterNot { choices.contains(it) }
            .ifEmpty { return null }

        val key = filtered.random()
        choices.add(key)
        val nodeKey = key.toNodeKey()
        return getAddPeer(nodeKey.name, nodeKey.host)
    }

    private fun eligiblePeers(): List<NodeKeyRepr> {
        val allPeers = clusterState.state.associateBy({ it.first }, { it.second })
        val thisKey = nodeKey.toString()
        // Shift over by COUNT_MAX to allow HLCTimestamp comparison
        val now = System.currentTimeMillis() * HLCTimestamp.COUNT_MAX
        val i = STARTUP_GRACE_PERIOD_SECONDS * 1000 * HLCTimestamp.COUNT_MAX

        return allPeers
            .filter { (key, ts) ->
                key != thisKey && (ts + i) < now && !suspects.contains(key)
            }
            .map { it.key }
    }

    private suspend fun gossipLoop() {
        while (!isStopped) {
            gossip()
            delayWithInterval(GOSSIP_INTERVAL_MS)
        }
    }

    private suspend fun gossip() {
        
    }

    private suspend fun investigationLoop() {}

    private suspend fun <R> failureDetection(peer: Peer, call: suspend () -> R) {
        try {
            call()
        } catch (e: Exception) {
            addSuspect(peer)
        }
    }

    private fun addSuspect(peer: Peer) {
        suspects.add(peer.nodeKey.toString())
        suspectQueue.add(peer)
        logger.info("membership.addSuspect ${peer.nodeKey}")
    }

    private suspend fun delayWithInterval(interval: Long) {
        val t = Random.nextLong(
            interval - JITTER_MS,
            interval + JITTER_MS
        )
        delay(t)
    }
}
