package dev.sankalp.orderflow.order

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import dev.sankalp.orderflow.common.events.EventHeaders
import dev.sankalp.orderflow.common.events.OrderCreated
import dev.sankalp.orderflow.common.events.PaymentCompleted
import dev.sankalp.orderflow.common.events.PaymentFailed
import dev.sankalp.orderflow.common.events.Topics
import dev.sankalp.orderflow.common.events.eventTypeOf
import dev.sankalp.orderflow.common.messaging.header
import dev.sankalp.orderflow.order.api.OrderResponse
import dev.sankalp.orderflow.order.api.PlaceOrderRequest
import dev.sankalp.orderflow.order.domain.OrderRepository
import dev.sankalp.orderflow.order.domain.OrderStatus
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.context.annotation.Import
import org.springframework.http.HttpStatus
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfig::class)
class OrderFlowIntegrationTest @Autowired constructor(
    private val rest: TestRestTemplate,
    private val kafka: KafkaTemplate<String, String>,
    private val consumerFactory: ConsumerFactory<String, String>,
    private val orders: OrderRepository,
    private val jdbc: JdbcTemplate,
    private val objectMapper: ObjectMapper,
) {

    @Test
    fun `placing an order publishes OrderCreated through the outbox`() {
        val orderId = placeOrder(BigDecimal("499.00"))

        val record = awaitRecord(Topics.ORDER_EVENTS) {
            it.key() == orderId.toString() && it.header(EventHeaders.EVENT_TYPE) == "OrderCreated"
        }

        val event = objectMapper.readValue<OrderCreated>(record.value())
        assertThat(event.orderId).isEqualTo(orderId)
        assertThat(event.amount).isEqualByComparingTo("499.00")
        assertThat(record.header(EventHeaders.EVENT_ID)).isNotBlank()
        await().atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThat(unpublishedOutboxRows(orderId)).isZero()
        }
    }

    @Test
    fun `a redelivered PaymentCompleted confirms the order exactly once`() {
        val orderId = placeOrder(BigDecimal("250.00"))
        val paymentId = UUID.randomUUID()
        val eventId = UUID.randomUUID()

        // Same eventId twice = Kafka redelivery after a consumer crash.
        repeat(2) { send(Topics.PAYMENT_EVENTS, orderId, eventId, PaymentCompleted(orderId, paymentId)) }

        await().atMost(Duration.ofSeconds(15)).untilAsserted {
            assertThat(orders.findById(orderId).get().status).isEqualTo(OrderStatus.CONFIRMED)
        }
        // Hold the assertion for a while so the duplicate has definitely been consumed.
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(6)).untilAsserted {
            assertThat(outboxRows(orderId, "OrderConfirmed")).isEqualTo(1)
        }
        assertThat(orders.findById(orderId).get().paymentId).isEqualTo(paymentId)
        assertThat(processedCount(eventId)).isEqualTo(1)
    }

    @Test
    fun `a failed payment cancels the order and a late success is ignored`() {
        val orderId = placeOrder(BigDecimal("99999.00"))

        send(Topics.PAYMENT_EVENTS, orderId, UUID.randomUUID(), PaymentFailed(orderId, "limit exceeded"))
        await().atMost(Duration.ofSeconds(15)).untilAsserted {
            val order = orders.findById(orderId).get()
            assertThat(order.status).isEqualTo(OrderStatus.CANCELLED)
            assertThat(order.failureReason).isEqualTo("limit exceeded")
        }

        // A different event arriving after the order is settled must not flip its state.
        val lateEventId = UUID.randomUUID()
        send(Topics.PAYMENT_EVENTS, orderId, lateEventId, PaymentCompleted(orderId, UUID.randomUUID()))
        await().atMost(Duration.ofSeconds(10)).until { processedCount(lateEventId) == 1 }

        assertThat(orders.findById(orderId).get().status).isEqualTo(OrderStatus.CANCELLED)
        assertThat(outboxRows(orderId, "OrderCancelled")).isEqualTo(1)
        assertThat(outboxRows(orderId, "OrderConfirmed")).isZero()
    }

    @Test
    fun `invalid requests are rejected and unknown orders return 404`() {
        val bad = rest.postForEntity("/orders", mapOf("customerId" to "", "amount" to 0), String::class.java)
        assertThat(bad.statusCode).isEqualTo(HttpStatus.BAD_REQUEST)

        val missing = rest.getForEntity("/orders/${UUID.randomUUID()}", String::class.java)
        assertThat(missing.statusCode).isEqualTo(HttpStatus.NOT_FOUND)
    }

    // ---- helpers ----

    private fun placeOrder(amount: BigDecimal): UUID {
        val response = rest.postForEntity(
            "/orders",
            PlaceOrderRequest(customerId = "cust-${UUID.randomUUID()}", amount = amount, currency = "INR"),
            OrderResponse::class.java,
        )
        assertThat(response.statusCode).isEqualTo(HttpStatus.ACCEPTED)
        val body = requireNotNull(response.body)
        assertThat(body.status).isEqualTo(OrderStatus.PENDING_PAYMENT)
        return body.id
    }

    private fun send(topic: String, key: UUID, eventId: UUID, event: Any) {
        val record = ProducerRecord(topic, key.toString(), objectMapper.writeValueAsString(event)).apply {
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

    private fun outboxRows(orderId: UUID, eventType: String): Int =
        jdbc.queryForObject(
            "select count(*) from outbox_events where message_key = ? and event_type = ?",
            Int::class.javaObjectType, orderId.toString(), eventType,
        ) ?: 0

    private fun unpublishedOutboxRows(orderId: UUID): Int =
        jdbc.queryForObject(
            "select count(*) from outbox_events where message_key = ? and published_at is null",
            Int::class.javaObjectType, orderId.toString(),
        ) ?: 0

    private fun processedCount(eventId: UUID): Int =
        jdbc.queryForObject(
            "select count(*) from processed_events where event_id = ?",
            Int::class.javaObjectType, eventId,
        ) ?: 0
}
