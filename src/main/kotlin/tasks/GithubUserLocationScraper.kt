package tasks

import infra.JacksonMapper
import io.github.cdimascio.dotenv.dotenv
import io.github.rybalkinsd.kohttp.dsl.httpGet
import io.github.rybalkinsd.kohttp.ext.url
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.runBlocking
import model.RateLimit
import model.User
import java.math.RoundingMode
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.time.Instant
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.milliseconds

class GithubUserLocationScraper {
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

    @ExperimentalTime
    @FlowPreview
    fun start() {
        runBlocking {
            var idx = 0
            var startTime: Instant? = null
            val throughputMeasureInterval = 30
            println("reading events with incomplete user data …")

            readCandidates()
                .flatMapMerge { result ->
                    checkRateLimit(++idx)

                    if (idx == 1) {
                        println("start fetching")
                        startTime = Instant.now()
                    }

                    if (idx % throughputMeasureInterval == 0) {
                        val diff4Ten = Instant.now().minusMillis(startTime!!.toEpochMilli()).toEpochMilli()
                        val throughput = throughputMeasureInterval.toDouble() / diff4Ten * 1000.0
                        println("${throughput.toBigDecimal().setScale(2, RoundingMode.HALF_EVEN)} users/sec")
                        startTime = Instant.now()
                    }

                    val userData = fetchUser(result.getString("username"))

                    when {
                        userData == null -> {
                            flowOf(null)
                        }
                        userData.location.isNullOrEmpty() -> {
                            // println("skip ${userData.login}, no location set")
                            flowOf(null)
                        }
                        else -> {
                            flowOf(userData)
                        }
                    }
                }
                .filterNotNull()
                .collect { user ->
                    val insertCount = storeUser(user)
                    if (insertCount > 0) {
                        println("stored $user")
                    }
                }
        }
    }

    @FlowPreview
    private fun readCandidates(): Flow<ResultSet> {
        // all events with no or incomplete user
        val query = conn.prepareStatement(
            """
select e1.userid, e1.username, u1.login stored_login, u1.id stored_userid, u1.location from 
	(select e.userid, e.username from events e
	 	where e.type = 'PushEvent'
		group by e.userid, e.username) e1
left outer join 
	(select u.id::text as id, u.login, u.location from users u) u1 
on u1.id = e1.userid
where u1.location is null
        """.trimIndent()
        )

        return flow {
            val result = query.executeQuery()
            while (result.next()) {
                emit(result)
            }
        }
    }

    @ExperimentalTime
    private suspend fun checkRateLimit(idx: Int) {
        val firstOrTenth = idx == 1 || idx % 10 == 0
        if (!firstOrTenth) {
            return
        }

        val response = httpGet {
            url("https://api.github.com/rate_limit")
            header {
                "Accept" to "application/vnd.github.v3+json"
                "Authorization" to "token ${env.get("GH_API_TOKEN")}"
            }
        }

        if (!response.isSuccessful) {
            throw IllegalStateException("rate_limit request failed: " + response.code())
        }

        val data = JacksonMapper.get().readValue(response.body()?.string(), RateLimit::class.java)

        val coreRateLimit = data.resources["core"]!!
        if (coreRateLimit.remaining < 250) {
            println(coreRateLimit)
            val delayMillis = Instant.ofEpochSecond(coreRateLimit.reset)
                .minusMillis(Instant.now().toEpochMilli())
                .toEpochMilli()
            println("wait for ${delayMillis.milliseconds}")
            delay(delayMillis)
        }
//        else {
//            println("remaining requests ${coreRateLimit.remaining}")
//        }
    }

    private fun fetchUser(username: String): User? {

        val userResponse = httpGet {
            url("https://api.github.com/users/$username")
            header {
                "Accept" to "application/vnd.github.v3+json"
                "Authorization" to "token " + env.get("GH_API_TOKEN")
            }
        }

        if (!userResponse.isSuccessful) {
            if (userResponse.code() == 404) {
                println("user $username does not exist anymore")
                return null
            }

            throw IllegalStateException("github users api request failed: " + userResponse.code())
        }

        return JacksonMapper.get().readValue(userResponse.body()?.string(), User::class.java);
    }

    private fun storeUser(user: User): Int {
        val command = conn.prepareStatement(
            """
            INSERT INTO users (id, login, location) VALUES (?, ?, ?)
                ON CONFLICT(id) DO
                    UPDATE SET location = ?;
            """.trimIndent()
        )
        command.setInt(1, user.id)
        command.setString(2, user.login)
        command.setString(3, user.location)
        command.setString(4, user.location)
        //            command.setDate(4, Date.valueOf(Instant.now().atZone(ZoneId.systemDefault()).toLocalDate()))
        //            command.setInt(5, userResponse.code())
        return command.executeUpdate()
    }
}
