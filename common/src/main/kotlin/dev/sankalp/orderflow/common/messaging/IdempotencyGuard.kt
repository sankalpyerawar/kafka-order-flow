package dev.sankalp.orderflow.common.messaging

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

/**
 * Kafka delivers at least once, so every consumer must tolerate duplicates.
 *
 * The guard records (eventId, consumer) in `processed_events` inside the same database
 * transaction as the business change. If the change rolls back, so does the marker, and
 * the redelivered message is processed again. If two copies arrive concurrently, the second
 * insert waits on the primary-key lock and then becomes a no-op.
 */
@Component
class IdempotencyGuard(private val jdbc: JdbcTemplate) {

    /** Returns true the first time this consumer sees [eventId]; false for duplicates. */
    @Transactional(propagation = Propagation.MANDATORY)
    fun firstTime(eventId: UUID, consumer: String): Boolean =
        jdbc.update(
            """
            insert into processed_events (event_id, consumer, processed_at)
            values (?, ?, now())
            on conflict do nothing
            """.trimIndent(),
            eventId,
            consumer,
        ) == 1
}
