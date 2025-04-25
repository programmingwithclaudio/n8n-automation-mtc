## Proyecto n8n TEST
[![Tema de Colores](https://img.shields.io/badge/theme-gruvbox%20dark-brightgreen)](https://github.com/morhetz/gruvbox)
[![Estado](https://img.shields.io/badge/estado-stand%20by-yellowgreen)](https://github.com/programmingwithclaudio/dotfiles)
[![Licencia](https://img.shields.io/badge/licencia-MIT-blue)](https://opensource.org/licenses/MIT)

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

1. **Utiliza el nombre del servicio en la red interna:**  
   Dado que ambos contenedores están en la misma red (`local_network`), debes emplear el nombre del servicio definido en el *docker-compose*. En este caso, en lugar de `http://localhost:5678/webhook/359869bf-edc3-43ec-89f7-444c16e01512`, utiliza:  
   ```
   http://n8n:5678/webhook/359869bf-edc3-43ec-89f7-444c16e01512
   ```  
   Esto le indica a Evolution API que debe enviar los eventos al contenedor n8n.

2. **Verifica la configuración de los parámetros de webhook:**  
   Observa que en la configuración actual tienes:  
   ```yaml
   - WEBHOOK_GLOBAL_ENABLED=false
   ```  
   Si la intención es que Evolution API escuche o envíe eventos de forma global, es posible que necesites establecerlo en `true` o revisar la documentación de Evolution API para confirmar si se requiere algún otro parámetro para gestionar las llamadas a webhook.

3. **Asegúrate de que la URL del servidor esté correctamente configurada:**  
   Actualmente, el parámetro `SERVER_URL` en Evolution API está configurado como `http://localhost:8080`. Si esta URL se utiliza internamente para construir enlaces o redirigir llamadas, es posible que debas ajustarla para reflejar la red interna o la dirección externa correcta según el flujo de trabajo que estés implementando.
---

#### Proyecto n8n TEST
[![bot-ia-n8n.png](https://i.postimg.cc/T1bSgmRR/bot-ia-n8n.png)](https://postimg.cc/hQSpqXzw)
- Flujo de nodos e intrucciones.


{
  "base": "El usuario {{ $('Edit Fields').item.json.sender.name }} {{ $('Edit Fields').item.json.recipient.full_jid }} dice: '{{ $('Edit Fields').item.json.message.content }}'."
  "contextInstructions": [
    "Responde de forma directa, cordial y profesional sin incluir encabezados ni indicaciones de generación automática.",
    "Proporciona solo la respuesta final en formato de texto sin etiquetas."
  ]
}

