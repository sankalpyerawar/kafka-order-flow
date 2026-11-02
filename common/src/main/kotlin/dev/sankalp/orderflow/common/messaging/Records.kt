package dev.sankalp.orderflow.common.messaging

import dev.sankalp.orderflow.common.events.EventHeaders
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.UUID

/**
 * A message that can never succeed, however often it is retried (bad JSON, missing headers).
 * The error handler sends these straight to the dead-letter topic without retrying.
 */
class PoisonMessageException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

fun ConsumerRecord<String, String>.header(name: String): String? =
    headers().lastHeader(name)?.value()?.toString(Charsets.UTF_8)

fun ConsumerRecord<String, String>.requireHeader(name: String): String =
    header(name) ?: throw PoisonMessageException("Missing header '$name' on ${coordinates()}")

fun ConsumerRecord<String, String>.eventId(): UUID {
    val raw = requireHeader(EventHeaders.EVENT_ID)
    return try {
        UUID.fromString(raw)
    } catch (e: IllegalArgumentException) {
        throw PoisonMessageException("Invalid eventId '$raw' on ${coordinates()}", e)
    }
}

fun ConsumerRecord<String, String>.eventType(): String = requireHeader(EventHeaders.EVENT_TYPE)

fun ConsumerRecord<String, String>.coordinates(): String = "${topic()}-${partition()}@${offset()}"
