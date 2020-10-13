import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.github.rybalkinsd.kohttp.dsl.context.ParamContext
import io.github.rybalkinsd.kohttp.dsl.httpGet
import io.github.rybalkinsd.kohttp.ext.url
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import okhttp3.Response
import java.io.FileWriter
import java.nio.file.Path

fun main(args: Array<String>) {
    /*
    gh-push-weather
        for push in gh-archive
            ghLocation = getUserLocation(push)
            noaaLocation = getConnectedLocation(ghLocation)
            dailyWeather = noaa.getWeather(noaaLocation, push.date)
            dump(dailyWeather, push, noaaLocation)

    gh-activity-weather
        measure push frequency e.g. per day per region.
        add weather info to each measurement
     */

    val mapper = JsonMapper.builder()
        .addModule(KotlinModule.Builder().nullToEmptyCollection(true).build())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).build()

    val token = "rZIxWMEzxVaSvdruxVjRxiFwvWPkFvCI"
    val baseUrl = "https://www.ncdc.noaa.gov/cdo-web/api/v2/"

    fun query(path: String, params: ParamContext.() -> Unit): Response {
        return httpGet {
            url(baseUrl + path)
            param(params)
            header {
                "token" to token
            }
        }.also { println(it.request().url()) }
    }

    fun <T : Any> queryAsync(path: String, dataType: Class<T>, params: ParamContext.() -> Unit): Flow<NOAAResponse<T>> = flow {
        var complete = false
        var offset = 1
        while(!complete){
            println("start position: $offset")

            val response = query(path) {
                params(this)
                "offset" to offset
            }

            if (!response.isSuccessful) {
                throw IllegalStateException(response.message())
            }

            val type: JavaType = mapper.typeFactory.constructParametricType(NOAAResponse::class.java, dataType)
            val data = mapper.readValue<NOAAResponse<T>>(response.body()?.string(), type)

            emit(data!!)

            val resultSet = data.metadata["resultset"]!!
            if(resultSet.count < resultSet.limit * resultSet.offset){
                complete = true
            } else {
                offset += resultSet.limit
                delay(250) // NOAA quota is 5 req/s
            }
        }
    }

    fun queryLocations(category: String): Flow<NOAAResponse<LocationCategory>> = flow {
        queryAsync("locations", LocationCategory::class.java) {
                "datasetid" to "GHCND"
                "locationcategoryid" to category
                "limit" to "1000"
            }.collect{ page ->
                emit(page)
            }
    }

    runBlocking {
        println("# Downloading countries\n")

        var path = Path.of("data/countries.json")
        FileWriter(path.toFile()).use { w ->
            queryLocations("CNTRY").collect { response ->
                val batch = response.results.joinToString("\n", "", "\n") {
                    mapper.writeValueAsString(it) }
                println(batch.substring(0, 120) + "\n")
                w.append(batch)
            }
        }

        println("# Downloading cities\n")

        path = Path.of("data/cities.json")
        FileWriter(path.toFile()).use { w ->
            queryLocations("CITY").collect { response ->
                val batch = response.results.joinToString("\n", "", "\n") {
                    mapper.writeValueAsString(it) }
                println(batch.substring(0, 120) + "\n")
                w.append(batch)
            }
        }
    }
}
