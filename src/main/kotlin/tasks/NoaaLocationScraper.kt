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

@OptIn(FlowPreview::class)
class NoaaLocationScraper {
    private val noaaClient = NoaaClient()

    fun start() {
        runBlocking {
            println("# Downloading countries\n")

            downloadLocations(Path.of("data/countries.json"), "CNTRY")

            println("# Downloading cities\n")

            downloadLocations(Path.of("data/cities.json"), "CITY")
        }
    }

    private suspend fun downloadLocations(dest: Path, category: String) {
        FileWriter(dest.toFile()).use { w ->
            queryLocations(category).collect { response ->
                val batch = response.results.joinToString("\n", "", "\n") {
                    JacksonMapper.get().writeValueAsString(it)
                }
                println(batch.substring(0, 120) + "\n")
                w.append(batch)
            }
        }
    }

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
