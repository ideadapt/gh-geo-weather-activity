package model

import java.util.*

data class Measurement(
    val date: Date,
    val datatype: String,
    val station: String,
    val value: Double,
) {
}
