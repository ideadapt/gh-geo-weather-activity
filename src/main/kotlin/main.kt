import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.required

fun main(args: Array<String>) {
    /*
    gh-weather-stats
        measure push frequency e.g. per day per region.
        add weather info to each measurement

Wieviel locations können von google geocoded werden?
    select data, count(*) c from locations group by data order by c desc
    4100 von 4617 aka 90%. ok.
Wieviel google geocoded locations können einer noaa location zugeordnet werden?
    178+93=270 von 4100 aka 6.5%. nok.
    => geocoded location müsste einer best match noaa location zugeordnet werden (z.B: umkreis, state, countries)

    geocoded cities mit join => 777
    geocoded cities ohne join => 4091

    geocoded countries mit join => 93
    geocoded countries ohne join => 139

    => ohne join zu noaa werden 4091+139=4230 geocoded locations als country+city eingestuft (von total 4300). ok.
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
