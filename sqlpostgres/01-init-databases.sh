#!/bin/bash
set -e
# Crear múltiples bases de datos
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE evolution2;
    CREATE DATABASE chatwoot;
    CREATE DATABASE n8n;
    
    GRANT ALL PRIVILEGES ON DATABASE evolution2 TO "$POSTGRES_USER";
    GRANT ALL PRIVILEGES ON DATABASE chatwoot TO "$POSTGRES_USER";
    GRANT ALL PRIVILEGES ON DATABASE n8n TO "$POSTGRES_USER";
EOSQL
# Script opcional para configuraciones adicionales
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d evolution2 <<-EOSQL
    -- Crear esquemas o usuarios adicionales si es necesario
    CREATE SCHEMA IF NOT EXISTS evolution_schema;
    
    -- Ejemplo de configuración de extensiones
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
EOSQL