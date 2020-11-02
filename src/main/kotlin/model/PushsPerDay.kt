package model

import java.sql.ResultSet
import java.time.format.DateTimeFormatter
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

    companion object {
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

        fun fromResult(result: ResultSet, startSql: java.sql.Date): PushsPerDay {
            return PushsPerDay(
                day = startSql,
                count = result.getInt("count"),
                country = result.getString("country") ?: "",
                country_nid = result.getString("country_nid") ?: "",
                city = result.getString("city") ?: "",
                city_nid = result.getString("city_nid") ?: "",
            )
        }
    }
}
