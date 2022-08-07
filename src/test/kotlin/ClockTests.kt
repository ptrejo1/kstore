
import com.google.protobuf.kotlin.toByteStringUtf8
import com.phoenix.kstore.KQL
import com.phoenix.kstore.grpc.DeleteRequest
import com.phoenix.kstore.grpc.GetRequest
import com.phoenix.kstore.grpc.Request
import com.phoenix.kstore.storage.*
import kotlinx.coroutines.*
import kotlin.test.Test

class ClockTests {

    private fun make(s: String): IndexEntry =
        IndexEntry(s.toByteArray(), 0)

    @Test
    fun testClock() {
        val tree = AVLTree()
        tree.insert(make("foo"))
        tree.insert(make("bar"))
        tree.insert(make("baz"))
        tree.insert(make("qux"))

        val s = tree.search("bar".toByteArray())
        println(s)

        val avg = mutableListOf<Long>()
        for (i in 0 until 1000) {
            val e = Entry("foobar".toByteArray(), "apple pie".toByteArray(), 1).encode()

            val q = System.nanoTime()
            Entry.decode(e)
            val w = System.nanoTime() - q
            avg.add(w / 100)
        }
        println(avg.average())
    }

    @Test
    fun testMem() {
        val m = MemTable(256)
        m.put(Entry("foo".toByteArray(), "bar".toByteArray(), 1))
        m.put(Entry("apple".toByteArray(), "pie".toByteArray(), 1))

        val r = m.get("apple".toByteArray())
        println(r!!.key.toString(Charsets.UTF_8))

        for (e in m.scan()) {
            println(e.key.toString(Charsets.UTF_8))
        }
    }

    @Test
    fun testRepl() {
        val s = Store()
        runBlocking {
            launch {
                val r = s.get("foo".toByteArray())
                println("r: ${r?.toString(Charsets.UTF_8)}")
            }
            launch { s.put("foo".toByteArray(), "bar".toByteArray()) }
            launch { s.put("foo".toByteArray(), "qux".toByteArray()) }
        }
//        runBlocking {
//            launch { s.put("foo".toByteArray(), "bar".toByteArray()) }
//        }
//        runBlocking {
//            launch { s.put("foo".toByteArray(), "qux".toByteArray()) }
//        }

        val r = runBlocking {
            s.get("foo".toByteArray())
        }
        println(r?.toString(Charsets.UTF_8))
    }

    @Test
    fun testRe() {
        val requestKeyRegex = """^/([A-Za-z\d]+)/([A-Za-z\d]+)${'$'}""".toRegex()
        val g = requestKeyRegex.matchEntire("/tab/123") ?: return
        println(g.groups)

        val f = com.phoenix.kstore.grpc.TransactionStatus.valueOf(TransactionStatus.COMMITTED.name)
        println(f)
    }

    @Test
    fun testParse() {
        val req = Request.newBuilder()
        req.setGet(GetRequest.newBuilder().setKey("".toByteStringUtf8()))
        req.setDelete(DeleteRequest.newBuilder().setKey("".toByteStringUtf8()))
        val f = req.build()
        println(f.hasDelete())
        println(f.hasGet())
    }
}
