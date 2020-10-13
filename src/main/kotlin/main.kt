import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.required

fun main(args: Array<String>) {
    /*
    gh-event-weather-scraper
        for push in pushs_per_day
            dailyWeather = noaa.getWeather(noaaLocation, push.date)
            dump(dailyWeather, push, noaaLocation)

    gh-weather-stats
        measure push frequency e.g. per day per region.
        add weather info to each measurement
     */

    val parser = ArgParser("gh-geo-weather-activity")
    val input by parser.option(
        ArgType.String, shortName = "s",
        fullName = "script",
        description = """Sub routine to run. One of:
            |   noaa-location-scraper   
            |   gh-event-weather-scraper
            |   
        """.trimMargin()
    ).required()
    parser.parse(args)

    when (input) {
        "noaa-location-scraper" -> {
            NoaaLocationScraper().start()
        }

        "gh-event-weather-scraper" -> {
        }

        else -> {
            println("invalid sub command")
        }
    }
}
