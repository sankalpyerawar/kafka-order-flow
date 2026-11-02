package dev.sankalp.orderflow.payment.application

import dev.sankalp.orderflow.common.events.OrderCreated
import dev.sankalp.orderflow.common.events.PaymentCompleted
import dev.sankalp.orderflow.common.events.PaymentFailed
import dev.sankalp.orderflow.common.events.Topics
import dev.sankalp.orderflow.common.messaging.IdempotencyGuard
import dev.sankalp.orderflow.common.outbox.OutboxWriter
import dev.sankalp.orderflow.payment.domain.Payment
import dev.sankalp.orderflow.payment.domain.PaymentRepository
import dev.sankalp.orderflow.payment.domain.PaymentStatus
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

@Service
class PaymentService(
    private val payments: PaymentRepository,
    private val gateway: PaymentGateway,
    private val outbox: OutboxWriter,
    private val idempotency: IdempotencyGuard,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Two layers of protection against double charging:
     * 1. the idempotency guard drops redelivered events (same eventId);
     * 2. the unique constraint on payments.order_id rejects a second payment for the same
     *    order even if it arrives under a different eventId (e.g. a producer bug).
     */
    @Transactional
    fun handleOrderCreated(eventId: UUID, event: OrderCreated) {
        if (!idempotency.firstTime(eventId, CONSUMER)) {
            log.info("Skipping duplicate event {}", eventId)
            return
        }
        payments.findByOrderId(event.orderId)?.let {
            log.warn("Order {} already has payment {}, ignoring event {}", event.orderId, it.id, eventId)
            return
        }

        val draft = when (val result = gateway.charge(event)) {
            ChargeResult.Approved -> Payment(event.orderId, event.amount, event.currency, PaymentStatus.COMPLETED)
            is ChargeResult.Declined -> Payment(event.orderId, event.amount, event.currency, PaymentStatus.FAILED, result.reason)
        }
        val payment = payments.save(draft)

        val paymentId = requireNotNull(payment.id)
        val outcome: Any = when (payment.status) {
            PaymentStatus.COMPLETED -> PaymentCompleted(event.orderId, paymentId)
            PaymentStatus.FAILED -> PaymentFailed(event.orderId, payment.failureReason ?: "Declined")
        }
        outbox.append(Topics.PAYMENT_EVENTS, key = event.orderId.toString(), event = outcome)
        log.info("Payment {} for order {}: {}", paymentId, event.orderId, payment.status)
    }

    companion object {
        const val CONSUMER = "payment-service.order-created"
    }
}
