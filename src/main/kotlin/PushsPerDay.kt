import java.util.*

data class PushsPerDay(
    val day: Date,
    val count: Int,
    val country: String,
    val city: String,
    val country_nid: String,
    val city_nid: String
) {
}
