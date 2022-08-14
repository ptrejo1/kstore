
import com.google.protobuf.kotlin.toByteStringUtf8
import com.phoenix.kstore.KQL
import com.phoenix.kstore.grpc.DeleteRequest
import com.phoenix.kstore.grpc.GetRequest
import com.phoenix.kstore.grpc.Request
import com.phoenix.kstore.storage.*
import kotlinx.coroutines.*
import kotlin.test.Test
import kotlin.test.assertEquals

class ClockTests {

    private fun make(s: String): IndexEntry =
        IndexEntry(s.toByteArray(), 0)

    @Test
    fun testClock() {
        val tree = AVLTree()
        tree.insert(make("foo1"))
        tree.insert(make("foo2"))
        tree.insert(make("foo5"))
        tree.insert(make("foo6"))

        var s = tree.search("foo8".toByteArray(), ComparisonType.EQ)
        var d = s?.key?.toString(Charsets.UTF_8)
        assertEquals(d, null)
        s = tree.search("foo1".toByteArray(), ComparisonType.EQ)
        d = s?.key?.toString(Charsets.UTF_8)
        assertEquals(d, "foo1")

        s = tree.search("foo8".toByteArray(), ComparisonType.LTE)
        d = s?.key?.toString(Charsets.UTF_8)
        assertEquals(d, "foo6")
        s = tree.search("foo6".toByteArray(), ComparisonType.LTE)
        d = s?.key?.toString(Charsets.UTF_8)
        assertEquals(d, "foo6")

        s = tree.search("foo3".toByteArray(), ComparisonType.GTE)
        d = s?.key?.toString(Charsets.UTF_8)
        assertEquals(d, "foo5")
        s = tree.search("foo2".toByteArray(), ComparisonType.GTE)
        d = s?.key?.toString(Charsets.UTF_8)
        assertEquals(d, "foo2")
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
