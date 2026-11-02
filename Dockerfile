# Builds either service from the monorepo: docker build --build-arg SERVICE=order-service .
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY . .
ARG SERVICE
RUN ./gradlew :${SERVICE}:bootJar --no-daemon -x test

FROM eclipse-temurin:21-jre
ARG SERVICE
WORKDIR /app
RUN useradd --system --uid 1001 app
COPY --from=build /src/${SERVICE}/build/libs/${SERVICE}-*.jar /app/app.jar
USER app
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
