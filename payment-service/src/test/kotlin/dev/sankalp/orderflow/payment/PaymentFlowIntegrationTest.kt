package dev.sankalp.orderflow.payment

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import dev.sankalp.orderflow.common.events.EventHeaders
import dev.sankalp.orderflow.common.events.OrderCreated
import dev.sankalp.orderflow.common.events.PaymentCompleted
import dev.sankalp.orderflow.common.events.PaymentFailed
import dev.sankalp.orderflow.common.events.Topics
import dev.sankalp.orderflow.common.events.eventTypeOf
import dev.sankalp.orderflow.common.messaging.header
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

@SpringBootTest
@Import(TestcontainersConfig::class)
class PaymentFlowIntegrationTest @Autowired constructor(
    private val kafka: KafkaTemplate<String, String>,
    private val consumerFactory: ConsumerFactory<String, String>,
    private val jdbc: JdbcTemplate,
    private val objectMapper: ObjectMapper,
) {

    @Test
    fun `an order within the limit is charged and PaymentCompleted is published`() {
        val orderId = UUID.randomUUID()
        sendOrderCreated(orderId, UUID.randomUUID(), BigDecimal("1200.00"))

        val record = awaitRecord(Topics.PAYMENT_EVENTS) { it.key() == orderId.toString() }

        assertThat(record.header(EventHeaders.EVENT_TYPE)).isEqualTo("PaymentCompleted")
        val event = objectMapper.readValue<PaymentCompleted>(record.value())
        assertThat(event.orderId).isEqualTo(orderId)
        assertThat(paymentStatus(orderId)).isEqualTo("COMPLETED")
    }

    @Test
    fun `an order over the limit is declined with PaymentFailed`() {
        val orderId = UUID.randomUUID()
        sendOrderCreated(orderId, UUID.randomUUID(), BigDecimal("50000.00"))

        val record = awaitRecord(Topics.PAYMENT_EVENTS) { it.key() == orderId.toString() }

        assertThat(record.header(EventHeaders.EVENT_TYPE)).isEqualTo("PaymentFailed")
        assertThat(objectMapper.readValue<PaymentFailed>(record.value()).reason).contains("exceeds limit")
        assertThat(paymentStatus(orderId)).isEqualTo("FAILED")
    }

    @Test
    fun `redelivered and re-sent OrderCreated events never double charge`() {
        val orderId = UUID.randomUUID()
        val eventId = UUID.randomUUID()

        sendOrderCreated(orderId, eventId, BigDecimal("300.00")) // original
        sendOrderCreated(orderId, eventId, BigDecimal("300.00")) // redelivery: same eventId
        sendOrderCreated(orderId, UUID.randomUUID(), BigDecimal("300.00")) // producer bug: new eventId, same order

        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(15)).untilAsserted {
            assertThat(count("select count(*) from payments where order_id = ?", orderId)).isEqualTo(1)
            assertThat(count("select count(*) from outbox_events where message_key = ?", orderId.toString())).isEqualTo(1)
        }
    }

    @Test
    fun `a poison message goes to the dead-letter topic instead of blocking the partition`() {
        val key = UUID.randomUUID().toString()
        val poison = ProducerRecord(Topics.ORDER_EVENTS, key, "{ this is not json").apply {
            headers().add(EventHeaders.EVENT_ID, UUID.randomUUID().toString().toByteArray())
            headers().add(EventHeaders.EVENT_TYPE, "OrderCreated".toByteArray())
        }
        kafka.send(poison).get(10, TimeUnit.SECONDS)

        val dead = awaitRecord(Topics.ORDER_EVENTS_DLT) { it.key() == key }
        assertThat(dead.value()).isEqualTo("{ this is not json")
        assertThat(dead.header("kafka_dlt-exception-fqcn")).isNotBlank()

        // The partition keeps flowing: a valid order sent afterwards with the same key is still processed.
        val orderId = UUID.fromString(key)
        sendOrderCreated(orderId, UUID.randomUUID(), BigDecimal("10.00"))
        await().atMost(Duration.ofSeconds(15)).untilAsserted {
            assertThat(paymentStatus(orderId)).isEqualTo("COMPLETED")
        }
    }

    // ---- helpers ----

    private fun sendOrderCreated(orderId: UUID, eventId: UUID, amount: BigDecimal) {
        val event = OrderCreated(orderId, "cust-1", amount, "INR")
        val record = ProducerRecord(Topics.ORDER_EVENTS, orderId.toString(), objectMapper.writeValueAsString(event)).apply {
            headers().add(EventHeaders.EVENT_ID, eventId.toString().toByteArray())
            headers().add(EventHeaders.EVENT_TYPE, eventTypeOf(event).toByteArray())
        }
        kafka.send(record).get(10, TimeUnit.SECONDS)
    }

    private fun awaitRecord(
        topic: String,
        match: (ConsumerRecord<String, String>) -> Boolean,
    ): ConsumerRecord<String, String> {
        consumerFactory.createConsumer("test-${UUID.randomUUID()}", "it").use { consumer ->
            consumer.subscribe(listOf(topic))
            val deadline = Instant.now().plusSeconds(20)
            while (Instant.now().isBefore(deadline)) {
                consumer.poll(Duration.ofMillis(250)).firstOrNull(match)?.let { return it }
            }
        }
        throw AssertionError("No matching record on $topic within 20s")
    }

    private fun paymentStatus(orderId: UUID): String? =
        jdbc.queryForList("select status from payments where order_id = ?", String::class.java, orderId).firstOrNull()

    private fun count(sql: String, vararg args: Any): Int =
        jdbc.queryForObject(sql, Int::class.javaObjectType, *args) ?: 0
}
