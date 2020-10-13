import io.github.cdimascio.dotenv.dotenv
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import java.sql.Connection
import java.sql.Date
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.*


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
        /**
        for push in pushs_per_day
        dailyWeather = noaa.getWeather(noaaLocation, push.date)
        dump(dailyWeather, push, noaaLocation)
         */
        runBlocking {
            readEvents().flatMapConcat { event ->

                // NCDC quota is max 5 req/s, we do 4 to stay safe
                delay(250)

                println("\n# Weather for ${event.locationId}\n")

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
                println(page.results.size.toString() + " " + page.results.take(10))
            }.collect { (event, page) ->
                val format = SimpleDateFormat("yyyy-MM-dd")

                page.results.forEach {
                    val sql =
                        "insert into location_weather (day, datatype, value, location_name, location_id) values (?, ?, ?, ?, ?);"
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
        val result = conn.createStatement().executeQuery("select * from pushs_per_day limit 200;")
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
