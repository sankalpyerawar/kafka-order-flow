package dev.sankalp.orderflow.common

import com.fasterxml.jackson.core.JsonProcessingException
import dev.sankalp.orderflow.common.messaging.PoisonMessageException
import dev.sankalp.orderflow.common.outbox.OutboxRelayProperties
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.listener.RetryListener
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.util.backoff.ExponentialBackOff

@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties(OutboxRelayProperties::class)
class MessagingConfig {

    private val log = LoggerFactory.getLogger(javaClass)

    /**
     * Transient failures (DB down, lock timeout) are retried with exponential backoff
     * while the partition is paused. After retries run out, or immediately for poison
     * messages, the record goes to `<topic>.DLT` on the same partition and the consumer
     * moves on, so one bad message never blocks the partition forever.
     *
     * Spring Boot wires this bean into the auto-configured listener container factory.
     */
    @Bean
    fun kafkaErrorHandler(template: KafkaTemplate<String, String>): DefaultErrorHandler {
        val recoverer = DeadLetterPublishingRecoverer(template)
        val backOff = ExponentialBackOff(200L, 2.0).apply { maxAttempts = 4 }
        return DefaultErrorHandler(recoverer, backOff).apply {
            addNotRetryableExceptions(
                PoisonMessageException::class.java,
                JsonProcessingException::class.java,
            )
            setRetryListeners(
                RetryListener { record, ex, attempt ->
                    log.warn(
                        "Delivery attempt {} failed for {}-{}@{}: {}",
                        attempt, record.topic(), record.partition(), record.offset(), ex.message,
                    )
                },
            )
        }
    }
}
