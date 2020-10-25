package model

data class ResultSet(
    val count: Int,
    val limit: Int,
    val offset: Int
) {
}

data class NoaaResponse<T>(
    val results: List<T>,
    val metadata: Map<String, ResultSet> = mapOf(
        Pair(
            "resultset", ResultSet(
                offset = 1,
                count = 0,
                limit = 0,
            )
        )
    )
) {
}
