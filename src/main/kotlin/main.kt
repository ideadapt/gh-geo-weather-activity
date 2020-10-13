import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.required

fun main(args: Array<String>) {
    /*
    gh-weather-stats
        measure push frequency e.g. per day per region.
        add weather info to each measurement

Wieviel events können von google geocoded werden? 276k von 8.7mio
Wieviel geocoded events können einer noaa location zugeordnet werden?
     */

    val parser = ArgParser("gh-geo-weather-activity")
    val input by parser.option(
        ArgType.String, shortName = "s",
        fullName = "script",
        description = """Sub routine to run. One of:
            |   noaa-location-scraper   
            |   event-weather-scraper
            |   
        """.trimMargin()
    ).required()
    parser.parse(args)

    when (input) {
        "noaa-location-scraper" -> {
            NoaaLocationScraper().start()
        }

        "event-weather-scraper" -> {
            EventWeatherScraper().start()
        }

        else -> {
            println("invalid sub command")
        }
    }
}
