
import com.phoenix.kstore.storage.AVLTree
import com.phoenix.kstore.storage.Entry
import com.phoenix.kstore.storage.IndexEntry
import com.phoenix.kstore.storage.MemTable
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
}
