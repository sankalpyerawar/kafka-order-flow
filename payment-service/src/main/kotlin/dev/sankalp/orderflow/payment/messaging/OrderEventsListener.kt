package dev.sankalp.orderflow.payment.messaging

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import dev.sankalp.orderflow.common.events.OrderCreated
import dev.sankalp.orderflow.common.events.Topics
import dev.sankalp.orderflow.common.messaging.eventId
import dev.sankalp.orderflow.common.messaging.eventType
import dev.sankalp.orderflow.payment.application.PaymentService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class OrderEventsListener(
    private val paymentService: PaymentService,
    private val objectMapper: ObjectMapper,
) {

    @KafkaListener(topics = [Topics.ORDER_EVENTS], groupId = "payment-service")
    fun onOrderEvent(record: ConsumerRecord<String, String>) {
        // orders.events also carries OrderConfirmed/OrderCancelled; payments only cares about new orders.
        if (record.eventType() != "OrderCreated") return
        val event = objectMapper.readValue<OrderCreated>(record.value())
        paymentService.handleOrderCreated(record.eventId(), event)
    }
}
