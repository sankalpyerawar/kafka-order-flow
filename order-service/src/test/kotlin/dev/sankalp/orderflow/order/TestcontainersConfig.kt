package dev.sankalp.orderflow.order

import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

/** Real Postgres and Kafka in Docker; Spring Boot wires the connection details automatically. */
@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfig {

    @Bean
    @ServiceConnection
    fun postgres(): PostgreSQLContainer<*> = PostgreSQLContainer<Nothing>(DockerImageName.parse("postgres:16-alpine"))

    @Bean
    @ServiceConnection
    fun kafka(): KafkaContainer = KafkaContainer(DockerImageName.parse("apache/kafka-native:3.9.0"))
}
