package dev.sankalp.orderflow.order.api

import dev.sankalp.orderflow.order.application.OrderNotFoundException
import dev.sankalp.orderflow.order.application.OrderService
import dev.sankalp.orderflow.order.application.PlaceOrder
import dev.sankalp.orderflow.order.domain.Order
import dev.sankalp.orderflow.order.domain.OrderStatus
import jakarta.validation.Valid
import jakarta.validation.constraints.DecimalMin
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Pattern
import org.springframework.http.HttpStatus
import org.springframework.http.ProblemDetail
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.math.BigDecimal
import java.net.URI
import java.time.Instant
import java.util.UUID

data class PlaceOrderRequest(
    @field:NotBlank
    val customerId: String,
    @field:DecimalMin(value = "0.01")
    val amount: BigDecimal,
    @field:Pattern(regexp = "[A-Z]{3}")
    val currency: String = "INR",
)

data class OrderResponse(
    val id: UUID,
    val customerId: String,
    val amount: BigDecimal,
    val currency: String,
    val status: OrderStatus,
    val paymentId: UUID?,
    val failureReason: String?,
    val createdAt: Instant,
    val updatedAt: Instant,
) {
    companion object {
        fun from(order: Order) = OrderResponse(
            id = requireNotNull(order.id),
            customerId = order.customerId,
            amount = order.amount,
            currency = order.currency,
            status = order.status,
            paymentId = order.paymentId,
            failureReason = order.failureReason,
            createdAt = order.createdAt,
            updatedAt = order.updatedAt,
        )
    }
}

@RestController
@RequestMapping("/orders")
class OrderController(private val orderService: OrderService) {

    /** Returns 202: the order is accepted, payment happens asynchronously. Poll GET for status. */
    @PostMapping
    fun place(@Valid @RequestBody request: PlaceOrderRequest): ResponseEntity<OrderResponse> {
        val order = orderService.placeOrder(PlaceOrder(request.customerId, request.amount, request.currency))
        return ResponseEntity.accepted()
            .location(URI.create("/orders/${order.id}"))
            .body(OrderResponse.from(order))
    }

    @GetMapping("/{id}")
    fun get(@PathVariable id: UUID): OrderResponse = OrderResponse.from(orderService.get(id))

    @ExceptionHandler(OrderNotFoundException::class)
    fun notFound(e: OrderNotFoundException): ProblemDetail =
        ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.message ?: "Not found")
}
