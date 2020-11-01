package tasks

import infra.NoaaClient
import io.github.cdimascio.dotenv.dotenv
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import model.PushsPerDay
import model.Weather
import java.sql.Connection
import java.sql.Date
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.*

/**
 * Store noaa weather data for each event (based location of user that created the event)
 */
class EventWeatherScraper {
    private val noaaClient = NoaaClient()
    private val env = dotenv()
    private val conn: Connection by lazy {
        val host = env.get("DB_HOST")
        val db = env.get("DB_NAME")
        val port = env.get("DB_PORT", "5432")
        val url = "jdbc:postgresql://$host:$port/$db"
        val props = Properties()
        props.setProperty("user", env.get("DB_USER"))
        props.setProperty("password", env.get("DB_PASSWORD"))

        DriverManager.getConnection(url, props)
    }

    @FlowPreview
    fun start() {
        runBlocking {
            readEvents().flatMapConcat { event ->

                // NCDC quota is max 5 req/s, we do 4 to stay safe
                delay(250)

                println("\nget weather for ${event.locationId}\n")

                val response = noaaClient.queryAsync("data", Weather::class.java) {
                    "datasetid" to "GHCND"
                    "locationid" to event.locationId
                    "units" to "metric"
                    "limit" to "25"
                    "startdate" to SimpleDateFormat("yyyy-MM-dd").format(event.day)
                    "enddate" to SimpleDateFormat("yyyy-MM-dd").format(event.day)
                }

                flowOf(event).zip(response) { e, r ->
                    e to r
                }
            }.onEach { (event, page) ->
                println(
                    page.results.size.toString() + " " + page.results.take(10).map { "${it.datatype}: ${it.value}" })
            }.map { (event, page) ->
                event to listOf(
                    (page.results.firstOrNull { it.datatype == "PRCP" } ?: Weather(
                        datatype = "PRCP",
                        date = event.day,
                        station = "",
                        value = Double.NaN
                    )),
                    (page.results.firstOrNull { it.datatype == "TAVG" }) ?: Weather(
                        datatype = "TAVG",
                        date = event.day,
                        station = "",
                        value = Double.NaN
                    )
                )
            }.collect { (event, measurements) ->
                val format = SimpleDateFormat("yyyy-MM-dd")

                measurements.forEach {
                    val sql =
                        """
                        INSERT INTO location_weather (day, datatype, value, location_name, location_id) VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT ON CONSTRAINT location_day 
                        DO NOTHING;
                        """.trimIndent()
                    val insert = conn.prepareStatement(sql)
                    insert.setDate(1, Date.valueOf(format.format(it.date)))
                    insert.setString(2, it.datatype)
                    insert.setDouble(3, it.value)
                    insert.setString(4, event.locationName)
                    insert.setString(5, event.locationId)
                    insert.executeUpdate()
                }
            }
        }
    }

    private fun readEvents(): Flow<PushsPerDay> {
        val dayEventsQry = conn.prepareStatement(
            """
            SELECT count(e.id),
                   u.location AS profile_location,
                   co.shortname AS country,
                   co.n_id AS country_nid,
                   ci.n_name AS city,
                   ci.n_id AS city_nid
            FROM events e
                     JOIN users u ON u.id::text = e.userid
                     FULL JOIN countries_in_profiles co ON co.profile_location = u.location
                     FULL JOIN cities_in_profiles ci ON ci.profile_location = u.location
            WHERE (co.n_id IS NOT NULL OR ci.n_id IS NOT NULL)
            AND createdat >= ? AND createdat < ?
            GROUP BY country, city, country_nid, city_nid, u.location
        """.trimIndent()
        )

        return flow {
            var startDate = LocalDate.of(2020, 2, 6)
            val endDate = LocalDate.of(2020, 3, 31)

            while (startDate < endDate) {
                emit(startDate)
                startDate = startDate.plusDays(1)
            }
        }.flatMapConcat { day ->
            val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            val startSql = Date.valueOf(formatter.format(day))
            val endSql = Date.valueOf(formatter.format(day.plusDays(1)))
            dayEventsQry.setDate(1, startSql)
            dayEventsQry.setDate(2, endSql)

            println("analyze ${startSql}..${endSql}")

            flow {
                val result = dayEventsQry.executeQuery()
                while (result.next()) {
                    val obj = PushsPerDay(
                        day = Date.valueOf(formatter.format(day)),
                        count = result.getInt("count"),
                        country = result.getString("country") ?: "",
                        country_nid = result.getString("country_nid") ?: "",
                        city = result.getString("city") ?: "",
                        city_nid = result.getString("city_nid") ?: "",
                    )
                    emit(obj)
                }
            }.filter {
                val locationWeatherQry = conn.prepareStatement(
                    """
             SELECT count(*) as count FROM location_weather lw WHERE
               lw.day >= ? AND lw.day < ?
               AND (lw.location_id = ? OR lw.location_id = ?)
                 """.trimIndent()
                )

                locationWeatherQry.setDate(1, startSql)
                locationWeatherQry.setDate(2, endSql)
                locationWeatherQry.setString(3, it.city_nid)
                locationWeatherQry.setString(4, it.country_nid)

                val result = locationWeatherQry.executeQuery()
                when {
                    result.next() && result.getInt("count") > 0 -> false
                    else -> true
                }
            }
        }
    }
}
