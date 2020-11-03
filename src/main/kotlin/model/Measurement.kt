package model

import java.time.LocalDateTime

data class Measurement(
    val date: LocalDateTime,
    val datatype: String,
    val station: String,
    val value: Double,
) {
}
