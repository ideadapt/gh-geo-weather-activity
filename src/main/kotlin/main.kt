import kotlinx.cli.ArgParser
import kotlinx.cli.ExperimentalCli
import kotlinx.cli.Subcommand
import kotlinx.coroutines.FlowPreview
import tasks.EventWeatherScraper
import tasks.GithubUserLocationScraper
import tasks.NoaaLocationScraper
import kotlin.time.ExperimentalTime

@FlowPreview
@ExperimentalCli
@ExperimentalTime
suspend fun main(args: Array<String>) {
    /*

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

    class EventWeatherScraperCommand :
        Subcommand("event-weather-scraper", "Download and store weather measurement data for already stored events.") {
        override fun execute() {
            EventWeatherScraper().start()
        }
    }

    class NoaaLocationScraperCommand :
        Subcommand("noaa-location-scraper", "Download and store all available cities and countries from NOAA.") {
        override fun execute() {
            NoaaLocationScraper().start()
        }
    }

    class GithubUserLocationScraperCommand : Subcommand(
        "github-user-location-scraper",
        "Geocode github user profile location (free text) and store it in a structured format."
    ) {
        override fun execute() {
            GithubUserLocationScraper().start()
        }
    }

    parser.subcommands(EventWeatherScraperCommand(), NoaaLocationScraperCommand(), GithubUserLocationScraperCommand())

    parser.parse(args)
}
