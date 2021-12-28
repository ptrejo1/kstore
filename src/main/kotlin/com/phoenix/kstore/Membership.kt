package com.phoenix.kstore

import com.phoenix.kstore.utils.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.math.min
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

    fun stop() {
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

    private suspend fun removePeer(peer: Peer) = mutex.withLock {
        clusterState.remove(peer.nodeKey)
        if (peers.contains(peer.nodeKey.name))
            peers.remove(peer.nodeKey.name)
        buildRouteTable()
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
        logger.info("membership.probe", peer.nodeKey)

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
        val nk = key.toNodeKey()
        return getAddPeer(nk.name, nk.host)
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
        val keys = getSubgroup(GOSSIP_SUBGROUP_SIZE)
            .ifEmpty { return }

        val gossipPeers = mutableListOf<Peer>()

        keys.forEach {
            val nk = it.toNodeKey()
            val peer = getAddPeer(nk.name, nk.host)
            gossipPeers.add(peer)
        }

        logger.info("membership.gossip", gossipPeers.map { it.nodeKey })

        // TODO: Check this is correct!
        gossipPeers.forEach {
            val task = coroutineScope { async { syncWithPeer(it) } }
            failureDetection(it) { task.await() }
        }
    }

    private suspend fun investigationLoop() {
        while (!isStopped) {
            val suspect = suspectQueue.poll() ?: break
            investigate(suspect)
        }
    }

    private suspend fun investigate(suspect: Peer) {
        val keys = getSubgroup(FAILURE_DETECTION_SUBGROUP_SIZE)
        val investigators = mutableListOf<Peer>()

        keys.forEach {
            val nk = it.toNodeKey()
            val peer = getAddPeer(nk.name, nk.host)
            investigators.add(peer)
        }

        logger.info(
            "membership.investigate",
            suspect.nodeKey,
            investigators.map { it.nodeKey }
        )

        val results = hashMapOf<Peer, Boolean>()

        // TODO: Check this is correct!
        investigators.forEach {
            val task = coroutineScope { async { it.pingReq(suspect) } }
            results[it] = task.await()
        }

        for ((peer, ack) in results) {
            if (!ack) continue
            failureVetoed(suspect, peer)
            return
        }

        failureConfirmed(suspect, investigators)
    }

    /** Another node was able to contact suspect */
    private fun failureVetoed(suspect: Peer, vetoingPeer: Peer) {
        logger.info("membership.failureVetoed", suspect.nodeKey, vetoingPeer.nodeKey)

        val suspectNodeKey = suspect.nodeKey.toString()
        if (suspects.contains(suspectNodeKey))
            suspects.remove(suspectNodeKey)
    }

    private suspend fun failureConfirmed(suspect: Peer, confirmingPeers: List<Peer>) {
        logger.info(
            "membership.failureConfirmed",
            suspect.nodeKey,
            confirmingPeers.map { it.nodeKey }
        )

        removePeer(suspect)

        val suspectNodeKey = suspect.nodeKey.toString()
        if (suspects.contains(suspectNodeKey))
            suspects.remove(suspectNodeKey)

        gossip()
    }

    private fun getSubgroup(size: Int): List<NodeKeyRepr> {
        val eligible = eligiblePeers().shuffled()
        val k = min(size, eligible.size)
        return eligible.subList(0, k)
    }

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
        logger.info("membership.addSuspect", peer.nodeKey)
    }

    private suspend fun delayWithInterval(interval: Long) {
        val t = Random.nextLong(
            interval - JITTER_MS,
            interval + JITTER_MS
        )
        delay(t)
    }
}
