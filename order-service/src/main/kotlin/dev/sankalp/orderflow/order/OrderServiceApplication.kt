package dev.sankalp.orderflow.order

import dev.sankalp.orderflow.common.events.Topics
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.config.TopicBuilder

// Scan the shared messaging module (outbox, idempotency, error handling) as well.
@SpringBootApplication(scanBasePackages = ["dev.sankalp.orderflow"])
class OrderServiceApplication {

    // The DLT must have as many partitions as its source topic: failed records keep their partition.
    @Bean fun orderEvents(): NewTopic = TopicBuilder.name(Topics.ORDER_EVENTS).partitions(3).replicas(1).build()
    @Bean fun paymentEvents(): NewTopic = TopicBuilder.name(Topics.PAYMENT_EVENTS).partitions(3).replicas(1).build()
    @Bean fun paymentEventsDlt(): NewTopic = TopicBuilder.name(Topics.PAYMENT_EVENTS_DLT).partitions(3).replicas(1).build()
}

fun main(args: Array<String>) {
    runApplication<OrderServiceApplication>(*args)
}
