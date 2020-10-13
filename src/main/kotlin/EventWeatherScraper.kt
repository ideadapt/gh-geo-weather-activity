import io.github.cdimascio.dotenv.dotenv
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import java.sql.Connection
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.*


class EventWeatherScraper {
    private val noaaClient = NoaaClient()
    private val env = dotenv()

    @FlowPreview
    fun start() {
        /**
        for push in pushs_per_day
        dailyWeather = noaa.getWeather(noaaLocation, push.date)
        dump(dailyWeather, push, noaaLocation)
         */
        runBlocking {
            readEvents().flatMapConcat { event ->

                val locationId = when (event.city_nid.isEmpty()) {
                    true -> event.country_nid
                    else -> event.city_nid
                }

                delay(250)

                println("\n# Weather for $locationId\n")

                noaaClient.queryAsync("data", Weather::class.java) {
                    "datasetid" to "GHCND"
                    "locationid" to locationId
                    "units" to "metric"
                    "limit" to "200"
                    "startdate" to SimpleDateFormat("yyyy-MM-dd").format(event.day)
                    "enddate" to SimpleDateFormat("yyyy-MM-dd").format(event.day)
                }
            }.collect { page ->
                println(page.results.take(2))
            }
        }
    }

    private fun readEvents(): Flow<PushsPerDay> {
        val host = env.get("DB_HOST")
        val db = env.get("DB_NAME")
        val port = env.get("DB_PORT", "5432")
        val url = "jdbc:postgresql://$host:$port/$db"
        val props = Properties()
        props.setProperty("user", env.get("DB_USER"))
        props.setProperty("password", env.get("DB_PASSWORD"))

        val conn: Connection = DriverManager.getConnection(url, props)!!
        val result = conn.createStatement().executeQuery("select * from pushs_per_day limit 10;")
        val format = SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

        return flow {
            while (result.next()) {
                val obj = PushsPerDay(
                    day = format.parse(result.getString("day")),
                    count = result.getInt("count"),
                    country = result.getString("country") ?: "",
                    country_nid = result.getString("country_nid") ?: "",
                    city = result.getString("city") ?: "",
                    city_nid = result.getString("city_nid") ?: "",
                )

                emit(obj)
            }
        }
    }
}
