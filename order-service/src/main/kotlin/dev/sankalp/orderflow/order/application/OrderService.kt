package dev.sankalp.orderflow.order.application

import dev.sankalp.orderflow.common.events.OrderCancelled
import dev.sankalp.orderflow.common.events.OrderConfirmed
import dev.sankalp.orderflow.common.events.OrderCreated
import dev.sankalp.orderflow.common.events.PaymentCompleted
import dev.sankalp.orderflow.common.events.PaymentFailed
import dev.sankalp.orderflow.common.events.Topics
import dev.sankalp.orderflow.common.messaging.IdempotencyGuard
import dev.sankalp.orderflow.common.outbox.OutboxWriter
import dev.sankalp.orderflow.order.domain.Order
import dev.sankalp.orderflow.order.domain.OrderRepository
import org.slf4j.LoggerFactory
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.math.BigDecimal
import java.util.UUID

data class PlaceOrder(val customerId: String, val amount: BigDecimal, val currency: String)

class OrderNotFoundException(id: UUID) : RuntimeException("Order $id not found")

@Service
class OrderService(
    private val orders: OrderRepository,
    private val outbox: OutboxWriter,
    private val idempotency: IdempotencyGuard,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    /** Saves the order and its OrderCreated event atomically (no dual write). */
    @Transactional
    fun placeOrder(command: PlaceOrder): Order {
        val order = orders.save(Order(command.customerId, command.amount, command.currency))
        val orderId = requireNotNull(order.id)
        outbox.append(
            Topics.ORDER_EVENTS,
            key = orderId.toString(),
            event = OrderCreated(orderId, order.customerId, order.amount, order.currency),
        )
        log.info("Order {} placed for {} {}", orderId, order.amount, order.currency)
        return order
    }

    @Transactional(readOnly = true)
    fun get(id: UUID): Order = orders.findByIdOrNull(id) ?: throw OrderNotFoundException(id)

    /**
     * Saga step: react to the payment outcome. Duplicates are dropped by the idempotency
     * guard; late or out-of-order results for an already-settled order are logged and
     * ignored rather than failing the consumer.
     */
    @Transactional
    fun applyPaymentResult(eventId: UUID, result: Any) {
        if (!idempotency.firstTime(eventId, CONSUMER)) {
            log.info("Skipping duplicate event {}", eventId)
            return
        }
        val orderId = when (result) {
            is PaymentCompleted -> result.orderId
            is PaymentFailed -> result.orderId
            else -> throw IllegalArgumentException("Unsupported payment result ${result::class}")
        }
        val order = orders.findByIdOrNull(orderId) ?: run {
            log.warn("Payment result {} for unknown order {}, ignoring", eventId, orderId)
            return
        }
        if (!order.isPending) {
            log.warn("Order {} already {}, ignoring {}", orderId, order.status, result::class.simpleName)
            return
        }

        val followUp: Any = when (result) {
            is PaymentCompleted -> {
                order.confirm(result.paymentId)
                OrderConfirmed(orderId, result.paymentId)
            }
            is PaymentFailed -> {
                order.cancel(result.reason)
                OrderCancelled(orderId, result.reason)
            }
            else -> error("unreachable")
        }
        outbox.append(Topics.ORDER_EVENTS, key = orderId.toString(), event = followUp)
        log.info("Order {} is now {}", orderId, order.status)
    }

    companion object {
        const val CONSUMER = "order-service.payment-results"
    }
}
