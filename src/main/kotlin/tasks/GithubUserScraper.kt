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
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.time.Instant
import java.util.*
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

class GithubUserScraper {
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
            readCandidates()
                .flatMapMerge { result ->
                    checkRateLimit(++idx)

                    val userData = fetchUser(result.getString("username"))
                    if (userData.location.isNullOrEmpty()) {
                        println("skip ${userData.login}, no location set")
                        flowOf(null)
                    } else {
                        flowOf(userData)
                    }
                }
                .filterNotNull()
                .collect { user ->
                    val insertCount = storeUser(user)
                    if (insertCount > 0) {
                        println("stored ${user}")
                    }
                }
        }
    }

    @FlowPreview
    private fun readCandidates(): Flow<ResultSet> {
        val query = conn.prepareStatement(
            """
           select (date_trunc('day', createdat))::timestamp, userid, username from events
           where type = 'PushEvent'
           group by 1, userid, username limit 100
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
        // only do for every tenth
        if (idx % 10 != 0) {
            return
        }

        val response = httpGet {
            url("https://api.github.com/rate_limit")
            header {
                "Accept" to "application/vnd.github.v3+json"
                "Authorization" to "token " + env.get("GH_API_TOKEN")
            }
        }

        if (!response.isSuccessful) {
            throw IllegalStateException("rate_limit request failed: " + response.code())
        }

        val data = JacksonMapper.get().readValue(response.body()?.string(), RateLimit::class.java)

        val coreRateLimit = data.resources["core"]!!
        if (coreRateLimit.remaining < 250) {
            val delayMillis = Instant.ofEpochMilli(coreRateLimit.reset)
                .minusMillis(Instant.now().toEpochMilli())
                .toEpochMilli()
            println("wait for ${delayMillis.seconds}")
            delay(delayMillis)
        } else {
            println("remaining requests ${coreRateLimit.remaining}")
        }
    }

    private fun fetchUser(username: String): User {

        val userResponse = httpGet {
            url("https://api.github.com/users/$username")
            header {
                "Accept" to "application/vnd.github.v3+json"
                "Authorization" to "token " + env.get("GH_API_TOKEN")
            }
        }

        if (!userResponse.isSuccessful) {
            throw IllegalStateException("users request failed: " + userResponse.code())
        }

        return JacksonMapper.get().readValue(userResponse.body()?.string(), User::class.java);
    }

    private fun storeUser(user: User): Int {
        val command = conn.prepareStatement(
            """
            INSERT INTO users (id, login, location) VALUES (?, ?, ?)
                ON CONFLICT(id) DO NOTHING;
            """.trimIndent()
        )
        command.setInt(1, user.id)
        command.setString(2, user.login)
        command.setString(3, user.location)
        //            command.setDate(4, Date.valueOf(Instant.now().atZone(ZoneId.systemDefault()).toLocalDate()))
        //            command.setInt(5, userResponse.code())
        return command.executeUpdate()
    }
}
