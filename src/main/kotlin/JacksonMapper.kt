import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule

object JacksonMapper {
    private val mapper: JsonMapper = JsonMapper.builder()
        .addModule(KotlinModule.Builder().nullToEmptyCollection(true).build())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).build();

    fun get(): JsonMapper {
        return mapper
    }
}
