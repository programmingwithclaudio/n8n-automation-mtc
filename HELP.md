En tu última prueba ya quedó claro que **Docker↔Host networking está funcionando** (el `{"message":"Workflow was started"}` venía de n8n): el golpe ahora es meramente de sintaxis de `curl` en **CMD de Windows** y de saber **dónde** probar.

---

### 1. Curl correcto en CMD de Windows

En Windows CMD **no** puedes partir líneas con `\` al estilo Unix, ni poner `-H` o `-d` aislados. Tienes que hacerlo **en una sola línea**, escapando las comillas del JSON:

```bat
curl -X POST "http://host.docker.internal:5678/webhook/agent-ia-dev" ^
  -H "Content-Type: application/json" ^
  -d "{\"foo\":\"bar\"}"
```

O aún más simple, todo pillado en una sola línea (sin `^`):

```bat
curl -X POST "http://host.docker.internal:5678/webhook/agent-ia-dev" -H "Content-Type: application/json" -d "{\"foo\":\"bar\"}"
```

**Explicación rápida**:

- `http://host.docker.internal:5678/...` — esta URL la resuelve Docker Desktop para que un contenedor (o tú si la pruebas en CMD) llegues al servicio que corre en tu Windows en el puerto 5678.  
- La parte `"{\"foo\":\"bar\"}"` es tu cuerpo JSON con comillas escapadas `\"`.

Si lo pegas tal cual en tu CMD, verás:

```json
{"message":"Workflow was started"}
```

y no “Bad hostname” ni errores de `-H` o `-d`.

---

### 2. Dónde probar cada URL

| Desde…                     | URL a usar                                                  |
|----------------------------|-------------------------------------------------------------|
| **Tu Windows CMD/Pow­erShell** | `http://localhost:5678/webhook/agent-ia-dev`               |
| **Dentro de un contenedor**    | `http://host.docker.internal:5678/webhook/agent-ia-dev` |

- **Host (Windows)**: usa `localhost`, porque ahí n8n está escuchando directamente.  
- **Contenedor**: usa `host.docker.internal`, que es el alias que Docker Desktop inyecta para apuntar al host.

---

### 3. Integración en tu `docker-compose`

En tu servicio de Evolution API, asegúrate de que la variable de entorno apunte a la dirección correcta **dentro del contenedor**:

```yaml
services:
  evolution_api_v2:
    # …
    environment:
      - WEBHOOK_URL=http://host.docker.internal:5678/webhook/agent-ia-dev
    # …
```

Luego:

1. `docker-compose down`  
2. `docker-compose up -d`

De esa forma, **cuando Evolution haga un POST a `$WEBHOOK_URL`**, lo enviará exitosamente a tu n8n local.

---

### 4. Verifica tu Webhook en n8n

1. Entra a http://localhost:5678, edita tu workflow de Webhook.  
2. Asegúrate de que el **Método HTTP** incluya `POST` (o `GET` si así lo configuras).  
3. Actívalo y prueba de nuevo desde tu contenedor o CMD según corresponda.

Con eso ya no tendrás `getaddrinfo ENOTFOUND` ni errores de sintaxis en `curl`. ¡Éxitos!

```bash
pip install "fastapi[standard]"

pip install aiohttp
pip install requests
nvm install v18.17.0
```
# https://www.youtube.com/watch?v=3KVqlIF0zZw&t=506s