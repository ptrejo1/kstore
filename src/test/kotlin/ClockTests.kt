
import com.phoenix.kstore.storage.AVLTree
import com.phoenix.kstore.storage.IndexEntry
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

        val f = make("bar")
        val s = tree.search(f)
        println(s)
    }
}
