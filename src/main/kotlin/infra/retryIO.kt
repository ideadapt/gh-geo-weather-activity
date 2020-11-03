package infra

import kotlinx.coroutines.delay
import java.io.IOException
import kotlin.time.ExperimentalTime
import kotlin.time.milliseconds
import kotlin.time.seconds

@OptIn(ExperimentalTime::class)
suspend fun <T> retry(
    times: Int = Int.MAX_VALUE,
    initialDelay: Long = 100.milliseconds.inMilliseconds.toLong(),
    maxDelay: Long = 1.seconds.inMilliseconds.toLong(),
    factor: Double = 2.0,
    block: suspend () -> T
): T {
    var currentDelay = initialDelay
    repeat(times - 1) {
        try {
            return block()
        } catch (e: IOException) {
            // you can log an error here and/or make a more finer-grained
            // analysis of the cause to see if retry is needed
            println("Exception occurred. Retry in ${currentDelay.milliseconds.inSeconds}s")
            println(e.localizedMessage)
        }
        delay(currentDelay)
        currentDelay = (currentDelay * factor).toLong().coerceAtMost(maxDelay)
    }
    return block() // last attempt
}
