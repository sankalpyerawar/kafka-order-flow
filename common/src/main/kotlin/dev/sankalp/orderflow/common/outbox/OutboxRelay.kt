package dev.sankalp.orderflow.common.outbox

import dev.sankalp.orderflow.common.events.EventHeaders
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.support.TransactionTemplate
import java.util.UUID
import java.util.concurrent.TimeUnit

@ConfigurationProperties("outbox.relay")
data class OutboxRelayProperties(
    val enabled: Boolean = true,
    val batchSize: Int = 100,
    val sendTimeoutMs: Long = 10_000,
)

/**
 * Polls unpublished outbox rows and sends them to Kafka.
 *
 * - `FOR UPDATE SKIP LOCKED` lets several service instances relay in parallel without
 *   publishing the same row twice at the same moment.
 * - Rows are sent in creation order and keyed by aggregate id, so events for one order
 *   land on one partition in order.
 * - Each send is awaited before the row is marked published. A crash between "sent" and
 *   "marked" re-sends the row on the next poll: delivery is at-least-once, which is why
 *   consumers use [dev.sankalp.orderflow.common.messaging.IdempotencyGuard].
 *
 * A production system might replace polling with CDC (Debezium reading the WAL).
 */
@Component
class OutboxRelay(
    private val jdbc: JdbcTemplate,
    private val kafka: KafkaTemplate<String, String>,
    private val tx: TransactionTemplate,
    private val props: OutboxRelayProperties,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${outbox.relay.poll-interval-ms:500}")
    fun poll() {
        if (!props.enabled) return
        try {
            var published: Int
            do {
                published = tx.execute { publishBatch() } ?: 0
            } while (published == props.batchSize)
        } catch (e: Exception) {
            // Rows stay unpublished and are retried on the next poll.
            log.warn("Outbox relay batch failed, will retry: {}", e.message)
        }
    }

    /** Must run inside a transaction so the row locks are held until rows are marked. */
    fun publishBatch(): Int {
        val pending = jdbc.query(
            """
            select id, topic, message_key, event_type, payload::text as payload
            from outbox_events
            where published_at is null
            order by created_at, id
            limit ?
            for update skip locked
            """.trimIndent(),
            RowMapper { rs, _ ->
                PendingEvent(
                    id = rs.getObject("id", UUID::class.java),
                    topic = rs.getString("topic"),
                    key = rs.getString("message_key"),
                    type = rs.getString("event_type"),
                    payload = rs.getString("payload"),
                )
            },
            props.batchSize,
        )
        if (pending.isEmpty()) return 0

        pending.forEach { event ->
            val record = ProducerRecord(event.topic, event.key, event.payload).apply {
                headers().add(EventHeaders.EVENT_ID, event.id.toString().toByteArray())
                headers().add(EventHeaders.EVENT_TYPE, event.type.toByteArray())
            }
            kafka.send(record).get(props.sendTimeoutMs, TimeUnit.MILLISECONDS)
        }

        jdbc.batchUpdate(
            "update outbox_events set published_at = now() where id = ?",
            pending.map { arrayOf<Any>(it.id) },
        )
        log.debug("Published {} outbox events", pending.size)
        return pending.size
    }

    private data class PendingEvent(
        val id: UUID,
        val topic: String,
        val key: String,
        val type: String,
        val payload: String,
    )
}
