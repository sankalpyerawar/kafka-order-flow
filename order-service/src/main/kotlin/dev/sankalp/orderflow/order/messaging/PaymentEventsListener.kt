package dev.sankalp.orderflow.order.messaging

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import dev.sankalp.orderflow.common.events.PaymentCompleted
import dev.sankalp.orderflow.common.events.PaymentFailed
import dev.sankalp.orderflow.common.events.Topics
import dev.sankalp.orderflow.common.messaging.eventId
import dev.sankalp.orderflow.common.messaging.eventType
import dev.sankalp.orderflow.order.application.OrderService
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class PaymentEventsListener(
    private val orderService: OrderService,
    private val objectMapper: ObjectMapper,
) {

    @KafkaListener(topics = [Topics.PAYMENT_EVENTS], groupId = "order-service")
    fun onPaymentEvent(record: ConsumerRecord<String, String>) {
        val eventId = record.eventId()
        val result: Any = when (record.eventType()) {
            "PaymentCompleted" -> objectMapper.readValue<PaymentCompleted>(record.value())
            "PaymentFailed" -> objectMapper.readValue<PaymentFailed>(record.value())
            else -> return // other event types on this topic are not ours to handle
        }
        orderService.applyPaymentResult(eventId, result)
    }
}
