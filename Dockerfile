FROM eclipse-temurin:23-jdk as builder

WORKDIR /app

COPY . .

RUN chmod +x ./gradlew && ./gradlew clean build -x test

FROM eclipse-temurin:23-jdk

WORKDIR /app

COPY --from=builder /app/build/libs/*.jar app.jar

EXPOSE 5001

ENTRYPOINT ["java", "-jar", "app.jar"]
