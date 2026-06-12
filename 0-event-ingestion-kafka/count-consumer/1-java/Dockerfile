# Multi-stage build: package the fat jar with Maven, ship it on a JRE image.
FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /app
COPY pom.xml ./
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app
COPY --from=builder /app/target/count-consumer-1.0.0.jar ./app.jar
EXPOSE 3001
CMD ["java", "-jar", "app.jar"]
