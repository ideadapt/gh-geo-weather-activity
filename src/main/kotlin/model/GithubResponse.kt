package model

data class User(
    val id: Int,
    val login: String,
    val location: String?
)

data class CoreRateLimit(
    val limit: Int,
    val remaining: Int,
    val reset: Long
)

data class RateLimit(
    val resources: Map<String, CoreRateLimit> = mapOf(
        Pair(
            "core", CoreRateLimit(
                limit = 1,
                remaining = 0,
                reset = 0
            )
        )
    )
)
