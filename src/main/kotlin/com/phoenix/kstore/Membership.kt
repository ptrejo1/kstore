package com.phoenix.kstore

import com.phoenix.kstore.utils.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.math.min
import kotlin.random.Random

suspend fun <T> inScopeAsync(block: suspend CoroutineScope.() -> T): Deferred<T> =
    coroutineScope { async { block() } }

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
    private var jobs: List<Job>
    private val jobsScope = CoroutineScope(Dispatchers.Default)

    init {
        runBlocking { clusterState.add(nodeKey) }
        buildRouteTable()

        jobs = listOf(
            jobsScope.launch(Dispatchers.Default, CoroutineStart.LAZY) {
                failureDetectionLoop()
            },
            jobsScope.launch(Dispatchers.Default, CoroutineStart.LAZY) {
                gossipLoop()
            },
            jobsScope.launch(Dispatchers.Default, CoroutineStart.LAZY) {
                investigationLoop()
            }
        )
    }

    /** Initial state sync */
    suspend fun bootstrap(peerNodeKey: NodeKey) {
        val peer = addPeer(peerNodeKey)
        syncWithPeer(peer)
    }

    fun start() {
        logger.info("start")
        jobs.forEach { it.start() }
    }

    fun stop() = runBlocking {
        isStopped = true
        jobs.forEach { it.cancelAndJoin() }
        logger.info("stop")
    }

    suspend fun pingRequest(peerNodeKey: NodeKey): Boolean {
        val peer = getAddPeer(peerNodeKey.name, peerNodeKey.host)
        val deferred = inScopeAsync { peer.ping() }
        return failureDetection(peer, deferred) ?: false
    }

    suspend fun stateSync(incoming: LWWRegister, host: Host): LWWRegister {
        getAddPeer(incoming.nodeName, host)

        mutex.withLock {
            clusterState.merge(incoming)
            buildRouteTable()
        }

        return clusterState
    }

    private suspend fun getAddPeer(nodeName: NodeName, host: Host): Peer =
        peers[nodeName] ?: addPeer(NodeKey(nodeName, host))

    private fun buildRouteTable() {
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

    /** @throws Exception if fails to reach peer */
    private suspend fun syncWithPeer(peer: Peer): LWWRegister {
        val peerMergedState = peer.stateSync(clusterState, nodeKey.host)
        return stateSync(peerMergedState, peer.nodeKey.host)
    }

    private suspend fun failureDetectionLoop() {
        while (!isStopped) {
            probeRandomPeer()
            delayWithInterval(FAILURE_DETECTION_INTERVAL_MS)
        }
    }

    private suspend fun probeRandomPeer() {
        val peer = getRandomPeer() ?: return
        logger.info("probe", peer.nodeKey)

        val deferred = inScopeAsync { peer.ping() }
        failureDetection(peer, deferred)
    }

    private suspend fun getRandomPeer(): Peer? {
        val eligiblePeers = eligiblePeers()

        if (choices.size >= eligiblePeers.size)
            choices = hashSetOf()

        val filtered = eligiblePeers
            .filterNot { choices.contains(it) }
            .ifEmpty { return null }

        val key = filtered.random()
        choices.add(key)
        val nk = key.toNodeKey()
        return getAddPeer(nk.name, nk.host)
    }

    private fun eligiblePeers(): List<NodeKeyRepr> {
        val allPeers = clusterState.state.toMap()
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
        val gossipPeers = keys.map {
            val nk = it.toNodeKey()
            getAddPeer(nk.name, nk.host)
        }

        logger.info("gossip", gossipPeers.map { it.nodeKey })

        val results = gossipPeers.associateWith {
            inScopeAsync { syncWithPeer(it) }
        }
        results.map { failureDetection(it.key, it.value) }
    }

    private suspend fun investigationLoop() {
        while (!isStopped) {
            val suspect = suspectQueue.poll() ?: break
            investigate(suspect)
        }
    }

    private suspend fun investigate(suspect: Peer) {
        val keys = getSubgroup(FAILURE_DETECTION_SUBGROUP_SIZE)
        val investigators = keys.map {
            val nk = it.toNodeKey()
            getAddPeer(nk.name, nk.host)
        }

        logger.info(
            "investigate",
            suspect.nodeKey,
            investigators.map { it.nodeKey }
        )

        val results = investigators
            .map { inScopeAsync { it.pingRequest(suspect) } }
            .map { runCatching { it.await() } }
            .map { it.getOrElse { false } }

        for ((peer, ack) in investigators.zip(results)) {
            if (!ack) continue
            failureVetoed(suspect, peer)
            return
        }

        failureConfirmed(suspect, investigators)
    }

    /** Another node was able to contact suspect */
    private fun failureVetoed(suspect: Peer, vetoingPeer: Peer) {
        logger.info("failureVetoed", suspect.nodeKey, vetoingPeer.nodeKey)

        val suspectNodeKey = suspect.nodeKey.toString()
        if (suspects.contains(suspectNodeKey))
            suspects.remove(suspectNodeKey)
    }

    private suspend fun failureConfirmed(suspect: Peer, confirmingPeers: List<Peer>) {
        logger.info(
            "failureConfirmed",
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

    private suspend fun <R> failureDetection(peer: Peer, deferred: Deferred<R>): R? {
        runCatching { deferred.await() }
            .onSuccess { return it }
            .onFailure { addSuspect(peer) }
        return null
    }

    private fun addSuspect(peer: Peer) {
        suspects.add(peer.nodeKey.toString())
        suspectQueue.add(peer)
        logger.info("addSuspect", peer.nodeKey)
    }

    private suspend fun delayWithInterval(interval: Long) {
        val t = Random.nextLong(
            interval - JITTER_MS,
            interval + JITTER_MS
        )
        delay(t)
    }
}
