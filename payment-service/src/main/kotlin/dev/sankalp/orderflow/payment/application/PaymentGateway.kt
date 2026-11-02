package dev.sankalp.orderflow.payment.application

import dev.sankalp.orderflow.common.events.OrderCreated
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component
import java.math.BigDecimal

sealed interface ChargeResult {
    data object Approved : ChargeResult
    data class Declined(val reason: String) : ChargeResult
}

/** Port to an external payment provider. */
fun interface PaymentGateway {
    fun charge(order: OrderCreated): ChargeResult
}

@ConfigurationProperties("payment.fake-gateway")
data class FakeGatewayProperties(
    val maxAmount: BigDecimal = BigDecimal("10000.00"),
)

/**
 * Deterministic stand-in for a real provider: approves anything up to [FakeGatewayProperties.maxAmount].
 * Deterministic on purpose, so tests and demos can trigger both saga outcomes.
 */
@Component
class FakePaymentGateway(private val props: FakeGatewayProperties) : PaymentGateway {
    override fun charge(order: OrderCreated): ChargeResult =
        if (order.amount <= props.maxAmount) {
            ChargeResult.Approved
        } else {
            ChargeResult.Declined("Amount ${order.amount} ${order.currency} exceeds limit ${props.maxAmount}")
        }
}
