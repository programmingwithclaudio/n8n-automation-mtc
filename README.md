## Proyecto n8n TEST


```bash
docker-compose up -d
chmod +x sqlpostgres/01-init-databases.sh
```

- [http://localhost:9001/login](http://localhost:9001/login)
- Login minioadmin - minioadmin
- [http://localhost:5678/setup](http://localhost:5678/setup)
- Creala por defecto
- [http://localhost:8080/](http://localhost:8080/)
```bash
{"status":200,"message":"Welcome to the Evolution API, it is working!","version":"2.2.0","clientName":"evolution_exchange","manager":"http://localhost:8080/manager","documentation":"https://doc.evolution-api.com"}
```
- [http://localhost:8080/manager](http://localhost:8080/manager)
- Login http://localhost:8080 - 6f452646de12e76ae1625de209d77862
## Probando




version: "3.8"

services:
  evolution_api_v2:
    image: atendai/evolution-api:v2.2.0
    volumes:
      - evolution_v2_instances:/evolution/instances
    networks:
      - traefik_public
      - general_network
    environment:
      - SERVER_URL=https://evoapi.tu-subdominio.com
      - DEL_INSTANCE=false
      - CORS_ORIGIN=*
      - CORS_METHODS=POST,GET,PUT,DELETE
      - CORS_CREDENTIALS=true
      - AUTHENTICATION_API_KEY=tu_contraseña_32_caracteres
      - AUTHENTICATION_EXPOSE_IN_FETCH_INSTANCES=true
      - LOG_LEVEL=ERROR,WARN
      - LOG_COLOR=true
      - LOG_BAILEYS=error
      - CONFIG_SESSION_PHONE_CLIENT=Evolution API V2
      - CONFIG_SESSION_PHONE_NAME=Chrome
      - CONFIG_SESSION_PHONE_VERSION=2.3000.1015901307
      - QRCODE_LIMIT=1902
      - QRCODE_COLOR=#000000
      - PROVIDER_ENABLED=false
      - PROVIDER_HOST=127.0.0.1
      - PROVIDER_PORT=5656
      - PROVIDER_PREFIX=evolution_api
      - DATABASE_ENABLED=true
      - DATABASE_PROVIDER=postgresql
      - DATABASE_CONNECTION_URI=postgresql://postgres:tu_contraseña_postgres@postgres:5432/evolution2
      - DATABASE_CONNECTION_CLIENT_NAME=evolution_api
      - DATABASE_SAVE_DATA_INSTANCE=true
      - DATABASE_SAVE_DATA_NEW_MESSAGE=true
      - DATABASE_SAVE_MESSAGE_UPDATE=true
      - DATABASE_SAVE_DATA_CONTACTS=true
      - DATABASE_SAVE_DATA_CHATS=true
      - RABBITMQ_ENABLED=true
      - RABBITMQ_URI=amqp://root:tu_contraseña_rabbitmq@rabbitmq:5672/default
      - RABBITMQ_EXCHANGE_NAME=evolution_api
      - RABBITMQ_GLOBAL_ENABLED=false
      - RABBITMQ_EVENTS_APPLICATION_STARTUP=true
      - RABBITMQ_EVENTS_INSTANCE_CREATE=true
      - RABBITMQ_EVENTS_INSTANCE_DELETE=true
      - RABBITMQ_EVENTS_QRCODE_UPDATED=true
      - RABBITMQ_EVENTS_MESSAGES_SET=true
      - RABBITMQ_EVENTS_MESSAGES_UPSERT=true
      - RABBITMQ_EVENTS_MESSAGES_EDITED=true
      - RABBITMQ_EVENTS_MESSAGES_UPDATE=true
      - RABBITMQ_EVENTS_MESSAGES_DELETE=true
      - RABBITMQ_EVENTS_SEND_MESSAGE=true
      - RABBITMQ_EVENTS_CONTACTS_SET=true
      - RABBITMQ_EVENTS_CONTACTS_UPSERT=true
      - RABBITMQ_EVENTS_CONTACTS_UPDATE=true
      - RABBITMQ_EVENTS_PRESENCE_UPDATE=true
      - RABBITMQ_EVENTS_CHATS_SET=true
      - RABBITMQ_EVENTS_CHATS_UPSERT=true
      - RABBITMQ_EVENTS_CHATS_UPDATE=true
      - RABBITMQ_EVENTS_CHATS_DELETE=true
      - RABBITMQ_EVENTS_GROUPS_UPSERT=true
      - RABBITMQ_EVENTS_GROUP_UPDATE=true
      - RABBITMQ_EVENTS_GROUP_PARTICIPANTS_UPDATE=true
      - RABBITMQ_EVENTS_CONNECTION_UPDATE=true
      - RABBITMQ_EVENTS_CALL=true
      - RABBITMQ_EVENTS_TYPEBOT_START=true
      - RABBITMQ_EVENTS_TYPEBOT_CHANGE_STATUS=true
      - SQS_ENABLED=false
      - SQS_ACCESS_KEY_ID=
      - SQS_SECRET_ACCESS_KEY=
      - SQS_ACCOUNT_ID=
      - SQS_REGION=
      - WEBSOCKET_ENABLED=false
      - WEBSOCKET_GLOBAL_EVENTS=false
      - WA_BUSINESS_TOKEN_WEBHOOK=evolution
      - WA_BUSINESS_URL=https://graph.facebook.com
      - WA_BUSINESS_VERSION=v20.0
      - WA_BUSINESS_LANGUAGE=en_US
      - WEBHOOK_GLOBAL_ENABLED=false
      - WEBHOOK_GLOBAL_URL=https://URL
      - WEBHOOK_GLOBAL_WEBHOOK_BY_EVENTS=false
      - WEBHOOK_EVENTS_APPLICATION_STARTUP=false
      - WEBHOOK_EVENTS_QRCODE_UPDATED=true
      - WEBHOOK_EVENTS_MESSAGES_SET=true
      - WEBHOOK_EVENTS_MESSAGES_UPSERT=true
      - WEBHOOK_EVENTS_MESSAGES_EDITED=true
      - WEBHOOK_EVENTS_MESSAGES_UPDATE=true
      - WEBHOOK_EVENTS_MESSAGES_DELETE=true
      - WEBHOOK_EVENTS_SEND_MESSAGE=true
      - WEBHOOK_EVENTS_CONTACTS_SET=true
      - WEBHOOK_EVENTS_CONTACTS_UPSERT=true
      - WEBHOOK_EVENTS_CONTACTS_UPDATE=true
      - WEBHOOK_EVENTS_PRESENCE_UPDATE=true
      - WEBHOOK_EVENTS_CHATS_SET=true
      - WEBHOOK_EVENTS_CHATS_UPSERT=true
      - WEBHOOK_EVENTS_CHATS_UPDATE=true
      - WEBHOOK_EVENTS_CHATS_DELETE=true
      - WEBHOOK_EVENTS_GROUPS_UPSERT=true
      - WEBHOOK_EVENTS_GROUPS_UPDATE=true
      - WEBHOOK_EVENTS_GROUP_PARTICIPANTS_UPDATE=true
      - WEBHOOK_EVENTS_CONNECTION_UPDATE=true
      - WEBHOOK_EVENTS_LABELS_EDIT=true
      - WEBHOOK_EVENTS_LABELS_ASSOCIATION=true
      - WEBHOOK_EVENTS_CALL=true
      - WEBHOOK_EVENTS_TYPEBOT_START=false
      - WEBHOOK_EVENTS_TYPEBOT_CHANGE_STATUS=false
      - WEBHOOK_EVENTS_ERRORS=false
      - WEBHOOK_EVENTS_ERRORS_WEBHOOK=https://url
      - OPENAI_ENABLED=true
      - DIFY_ENABLED=true
      - TYPEBOT_ENABLED=true
      - TYPEBOT_SEND_MEDIA_BASE64=true
      - TYPEBOT_API_VERSION=latest
      - CHATWOOT_ENABLED=true
      - CHATWOOT_MESSAGE_READ=true
      - CHATWOOT_MESSAGE_DELETE=false
      - CHATWOOT_IMPORT_DATABASE_CONNECTION_URI=postgresql://postgres:tu_contraseña_postgres@postgres:5432/chatwoot?sslmode=disable
      - CHATWOOT_IMPORT_PLACEHOLDER_MEDIA_MESSAGE=true
      - LANGUAGE=es
      - CACHE_REDIS_ENABLED=true
      - CACHE_REDIS_URI=redis://redis:6379/6
      - CACHE_REDIS_PREFIX_KEY=evolution_api
      - CACHE_REDIS_SAVE_INSTANCES=false
      - CACHE_LOCAL_ENABLED=false
      - S3_ENABLED=true
      - S3_ACCESS_KEY=acces_key
      - S3_SECRET_KEY=secret_key
      - S3_BUCKET=typebot
      - S3_PORT=443
      - S3_ENDPOINT=minioback.tu-subdominio.com
      - S3_USE_SSL=true
      - LANGUAGE=en
    deploy:
      mode: replicated
      replicas: 1
      placement:
        constraints:
          - node.role == manager
      resources:
        limits:
          cpus: "1"
          memory: 1024M
      labels:
        - traefik.enable=true
        - traefik.http.routers.evolution_api_v2.rule=Host(`evoapi.tu-subdominio.com`)
        - traefik.http.routers.evolution_api_v2.entrypoints=websecure
        - traefik.http.routers.evolution_api_v2.tls.certresolver=le
        - traefik.http.routers.evolution_api_v2.priority=1
        - traefik.http.routers.evolution_api_v2.service=evolution_api_v2
        - traefik.http.services.evolution_api_v2.loadbalancer.server.port=8080
        - traefik.http.services.evolution_api_v2.loadbalancer.passHostHeader=true

  minio:
    image: minio/minio:latest
    container_name: minio
    restart: unless-stopped
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data
    networks:
      - bigdata-network
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 20s
      retries: 3

volumes:
  evolution_v2_instances:
    external: true
    name: evolution_v2_instances

networks:
  traefik_public:
    external: true
  general_network:
    external: true