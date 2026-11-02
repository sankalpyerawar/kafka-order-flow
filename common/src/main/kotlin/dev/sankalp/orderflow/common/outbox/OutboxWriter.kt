package dev.sankalp.orderflow.common.outbox

import com.fasterxml.jackson.databind.ObjectMapper
import dev.sankalp.orderflow.common.events.eventTypeOf
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

/**
 * Transactional outbox: instead of calling Kafka inside a business transaction (dual write),
 * we insert the event into `outbox_events` in the SAME transaction as the state change.
 * Either both commit or neither does. [OutboxRelay] publishes committed rows afterwards.
 *
 * The outbox row id doubles as the event id that consumers use for deduplication.
 */
@Component
class OutboxWriter(
    private val jdbc: JdbcTemplate,
    private val objectMapper: ObjectMapper,
) {

    @Transactional(propagation = Propagation.MANDATORY)
    fun append(topic: String, key: String, event: Any): UUID {
        val eventId = UUID.randomUUID()
        jdbc.update(
            """
            insert into outbox_events (id, topic, message_key, event_type, payload, created_at)
            values (?, ?, ?, ?, ?::jsonb, now())
            """.trimIndent(),
            eventId,
            topic,
            key,
            eventTypeOf(event),
            objectMapper.writeValueAsString(event),
        )
        return eventId
    }
}
