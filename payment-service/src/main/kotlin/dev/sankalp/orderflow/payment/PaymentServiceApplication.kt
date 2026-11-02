package dev.sankalp.orderflow.payment

import dev.sankalp.orderflow.common.events.Topics
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.config.TopicBuilder

@SpringBootApplication(scanBasePackages = ["dev.sankalp.orderflow"])
@ConfigurationPropertiesScan
class PaymentServiceApplication {

    @Bean fun orderEvents(): NewTopic = TopicBuilder.name(Topics.ORDER_EVENTS).partitions(3).replicas(1).build()
    @Bean fun orderEventsDlt(): NewTopic = TopicBuilder.name(Topics.ORDER_EVENTS_DLT).partitions(3).replicas(1).build()
    @Bean fun paymentEvents(): NewTopic = TopicBuilder.name(Topics.PAYMENT_EVENTS).partitions(3).replicas(1).build()
}

fun main(args: Array<String>) {
    runApplication<PaymentServiceApplication>(*args)
}
