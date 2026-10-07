# syntax=docker/dockerfile:1

FROM gradle:8.14-jdk21 AS builder

WORKDIR /app

# Copiamos únicamente los archivos necesarios para descargar dependencias primero
COPY settings.gradle.kts build.gradle.kts ./
COPY gradle gradle

RUN --mount=type=cache,target=/home/gradle/.gradle gradle --no-build-cache --version

# Descargamos dependencias con cache
RUN --mount=type=cache,target=/home/gradle/.gradle gradle build -x test --no-build-cache 2>/dev/null || true

# Copiamos el resto del proyecto
COPY . .

# Compilamos y empaquetamos la aplicación Spring Boot (no ejecutamos tests)
RUN --mount=type=cache,target=/home/gradle/.gradle gradle clean bootJar -x test --no-build-cache

# Imagen de ejecución
FROM eclipse-temurin:21-jre-alpine

RUN apk --no-cache add curl

WORKDIR /app

# Copiamos el JAR generado
COPY --from=builder /app/build/libs/order-processor-*.jar app.jar

# Puerto por defecto de Spring Boot (aunque no expone API REST; por completitud)
EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]