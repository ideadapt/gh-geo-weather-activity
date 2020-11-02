package infra

import com.fasterxml.jackson.databind.JavaType
import io.github.cdimascio.dotenv.dotenv
import io.github.rybalkinsd.kohttp.client.client
import io.github.rybalkinsd.kohttp.dsl.context.ParamContext
import io.github.rybalkinsd.kohttp.dsl.httpGet
import io.github.rybalkinsd.kohttp.ext.url
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import model.NoaaResponse
import okhttp3.Response

class NoaaClient {
    private val baseUrl = "https://www.ncdc.noaa.gov/cdo-web/api/v2/"
    private val env = dotenv()
    private val token = env.get("GH_NOAA_TOKEN")

    private fun query(path: String, params: ParamContext.() -> Unit): Response {
        return httpGet(client {
            readTimeout = 30_000
        }) {
            url(baseUrl + path)
            param(params)
            header {
                "token" to token
            }
        }.also { println(it.request().url()) }
    }

    fun <T : Any> queryAsync(path: String, dataType: Class<T>, params: ParamContext.() -> Unit): Flow<NoaaResponse<T>> =
        flow {
            var complete = false
            var offset = 1
            while (!complete) {
                println("start position: $offset")

                val response = retry(times = 3) {
                    query(path) {
                        params(this)
                        "offset" to offset
                    }
                }

                if (!response.isSuccessful) {
                    throw IllegalStateException("HTTP response: ${response.code()} ${response.message()}")
                }

                val type: JavaType =
                    JacksonMapper.get().typeFactory.constructParametricType(NoaaResponse::class.java, dataType)
                val data = JacksonMapper.get().readValue<NoaaResponse<T>>(response.body()?.string(), type)

                emit(data!!)

                val resultSet = data.metadata["resultset"]!!
                if (resultSet.count < resultSet.limit * resultSet.offset) {
                    complete = true
                } else {
                    offset += resultSet.limit
                    delay(250) // NOAA quota is 5 req/s
                }
            }
        }
}
