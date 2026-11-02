package dev.sankalp.orderflow.order.domain

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import jakarta.persistence.Version
import org.springframework.data.jpa.repository.JpaRepository
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

enum class OrderStatus { PENDING_PAYMENT, CONFIRMED, CANCELLED }

@Entity
@Table(name = "orders")
class Order(
    @Column(name = "customer_id", nullable = false)
    var customerId: String,

    @Column(nullable = false, precision = 19, scale = 2)
    var amount: BigDecimal,

    @Column(nullable = false, length = 3)
    var currency: String,
) {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    var id: UUID? = null

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    var status: OrderStatus = OrderStatus.PENDING_PAYMENT
        protected set

    @Column(name = "payment_id")
    var paymentId: UUID? = null
        protected set

    @Column(name = "failure_reason")
    var failureReason: String? = null
        protected set

    @Column(name = "created_at", nullable = false)
    var createdAt: Instant = Instant.now()

    @Column(name = "updated_at", nullable = false)
    var updatedAt: Instant = Instant.now()

    @Version
    var version: Long = 0

    val isPending: Boolean get() = status == OrderStatus.PENDING_PAYMENT

    fun confirm(paymentId: UUID) {
        check(isPending) { "Order $id is $status, cannot confirm" }
        this.status = OrderStatus.CONFIRMED
        this.paymentId = paymentId
        this.updatedAt = Instant.now()
    }

    fun cancel(reason: String) {
        check(isPending) { "Order $id is $status, cannot cancel" }
        this.status = OrderStatus.CANCELLED
        this.failureReason = reason
        this.updatedAt = Instant.now()
    }
}

interface OrderRepository : JpaRepository<Order, UUID>
