package tasks

import infra.JacksonMapper
import infra.NoaaClient
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import model.LocationCategory
import model.NoaaResponse
import java.io.FileWriter
import java.nio.file.Path

class NoaaLocationScraper {
    private val noaaClient = NoaaClient()

    @FlowPreview
    fun start() {
        runBlocking {
            println("# Downloading countries\n")

            var path = Path.of("data/countries.json")
            FileWriter(path.toFile()).use { w ->
                queryLocations("CNTRY").collect { response ->
                    val batch = response.results.joinToString("\n", "", "\n") {
                        JacksonMapper.get().writeValueAsString(it)
                    }
                    println(batch.substring(0, 120) + "\n")
                    w.append(batch)
                }
            }

            println("# Downloading cities\n")

            path = Path.of("data/cities.json")
            FileWriter(path.toFile()).use { w ->
                queryLocations("CITY").collect { response ->
                    val batch = response.results.joinToString("\n", "", "\n") {
                        JacksonMapper.get().writeValueAsString(it)
                    }
                    println(batch.substring(0, 120) + "\n")
                    w.append(batch)
                }
            }
        }
    }

    @FlowPreview
    private fun queryLocations(category: String): Flow<NoaaResponse<LocationCategory>> = flow {
        noaaClient.queryAsync("locations", LocationCategory::class.java) {
            "datasetid" to "GHCND"
            "locationcategoryid" to category
            "limit" to "1000"
        }.collect { page ->
            emit(page)
        }
    }
}
