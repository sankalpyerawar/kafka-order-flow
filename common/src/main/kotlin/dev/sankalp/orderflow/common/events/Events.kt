package dev.sankalp.orderflow.common.events

import java.math.BigDecimal
import java.util.UUID

object Topics {
    const val ORDER_EVENTS = "orders.events"
    const val PAYMENT_EVENTS = "payments.events"
    const val ORDER_EVENTS_DLT = "$ORDER_EVENTS.DLT"
    const val PAYMENT_EVENTS_DLT = "$PAYMENT_EVENTS.DLT"
}

/** Every message carries these headers; the record value is the JSON payload only. */
object EventHeaders {
    const val EVENT_ID = "eventId"
    const val EVENT_TYPE = "eventType"
}

// ---- orders.events (key = orderId) ----

data class OrderCreated(
    val orderId: UUID,
    val customerId: String,
    val amount: BigDecimal,
    val currency: String,
)

data class OrderConfirmed(val orderId: UUID, val paymentId: UUID)

data class OrderCancelled(val orderId: UUID, val reason: String)

// ---- payments.events (key = orderId) ----

data class PaymentCompleted(val orderId: UUID, val paymentId: UUID)

data class PaymentFailed(val orderId: UUID, val reason: String)

/** The event type written to the header is the simple class name, e.g. "OrderCreated". */
fun eventTypeOf(event: Any): String = event::class.simpleName
    ?: error("Events must be named classes, got ${event::class}")
