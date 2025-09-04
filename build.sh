#!/bin/bash

# Define el nombre del servicio de la base de datos de Docker Compose
DB_SERVICE="tp1-db-1"

echo "Iniciando el servicio de base de datos MySQL con Docker Compose..."
docker compose -f msql.yml up -d

# Espera a que la base de datos esté lista
until docker exec "$DB_SERVICE" mysqladmin ping -hlocalhost --silent; do
    echo "Esperando a que la base de datos se inicie..."
    sleep 2
done

echo "Base de datos iniciada. Construyendo la imagen de la aplicación Java..."
# Construye la imagen de la aplicación desde el Dockerfile
docker build -t integrador-app .

echo "Ejecutando el contenedor de la aplicación..."
# Ejecuta la aplicación en un contenedor separado y vinculado a la red de la base de datos
docker run --rm --name integrador-app-run --network=host integrador-app

echo "Proceso finalizado. Puedes detener la base de datos con: docker compose -f msql.yml down"
