import java.util.*

data class PushsPerDay(
    val day: Date,
    val count: Int,
    val country: String,
    val city: String,
    val country_nid: String,
    val city_nid: String
) {
    val locationId: String by lazy {
        when (city_nid.isEmpty()) {
            true -> country_nid
            else -> city_nid
        }
    }

    val locationName: String by lazy {
        when (city.isEmpty()) {
            true -> country
            else -> city
        }
    }
}
