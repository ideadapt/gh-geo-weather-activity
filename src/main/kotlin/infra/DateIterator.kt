package infra

import java.sql.PreparedStatement
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

operator fun ClosedRange<LocalDate>.iterator(): Iterator<LocalDate> {
    return object : Iterator<LocalDate> {
        private var next = this@iterator.start
        private val finalElement = this@iterator.endInclusive
        private var hasNext = !next.isAfter(this@iterator.endInclusive)
        override fun hasNext(): Boolean = hasNext

        override fun next(): LocalDate {
            val value = next
            if (value == finalElement) {
                hasNext = false
            } else {
                next = next.plusDays(1)
            }
            return value
        }
    }
}

fun PreparedStatement.setLocalDateTime(parameterIndex: Int, date: LocalDateTime) {
    this.setObject(
        parameterIndex,
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(date),
        java.sql.Types.TIMESTAMP
    )
}
