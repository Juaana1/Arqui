# Usa una imagen base de Maven para construir la aplicación
FROM maven:3.9.6-eclipse-temurin-17 AS builder

# Establece el directorio de trabajo
WORKDIR /app

# Copia el archivo pom.xml para que Maven descargue las dependencias
COPY pom.xml .

# Copia el código fuente
COPY src ./src

# Compila y empaqueta la aplicación en un JAR ejecutable
RUN mvn clean package

# Usa una imagen base de OpenJDK para ejecutar la aplicación, más ligera
FROM openjdk:17-jdk-slim

# Establece el directorio de trabajo
WORKDIR /app

# Copia los archivos CSV
COPY clientes.csv .
COPY facturas-productos.csv .
COPY facturas.csv .
COPY productos.csv .

# Copia el archivo JAR compilado con dependencias desde la etapa "builder"
COPY --from=builder /app/target/app-with-dependencies.jar ./app.jar

# Define el comando para ejecutar la aplicación
CMD ["java", "-jar", "app.jar"]
