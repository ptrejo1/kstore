
import com.phoenix.kstore.Maglev
import kotlinx.coroutines.runBlocking
import kotlin.test.Test

class ClockTests {

    @Test
    fun testClock() = runBlocking {
        val m = Maglev(listOf("123b1h231=1123123", "abc123"))
        val oo = listOf("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")

        for (o in oo) {
            val l = m.lookup(o)
            println(l)
        }
    }
}
