nodo 1 edit file=
```bash
{
  "message_id": "{{$json.body.data.key.id}}",
  "recipient": {
    "phone": "{{$json.body.data.key.remoteJid.split('@')[0]}}",
    "full_jid": "{{$json.body.data.key.remoteJid}}"
  },
  "sender": {
    "phone": "{{$json.body.sender.split('@')[0]}}",
    "name": "{{$json.body.data.pushName}}",
    "device": "{{$json.body.data.source}}"
  },
  "message": {
    "content": "{{$json.body.data.message.conversation}}",
    "type": "{{$json.body.data.messageType}}",
    "timestamp": "{{$json.body.data.messageTimestamp}}",
    "security": {
      "encryption": "{{$json.body.data.message.messageContextInfo.deviceListMetadata.senderAccountType}}",
      "secret": "{{$json.body.data.message.messageContextInfo.messageSecret}}"
    }
  },
  "instance": {
    "name": "{{$json.body.instance}}",
    "server": "{{$json.body.server_url}}",
    "api_key": "{{$json.body.apikey}}"
  },
  "context": {
    "session_id": "{{$json.body.data.key.id}}_{{$json.body.data.messageTimestamp}}",
    "origin": "{{$json.body.data.source}}"
  }
}

```
nodo 2 agente ia =
```bash
{
  "base": "El usuario {{ $json.sender.name }} ({{ $json.recipient.full_jid }}) dice: '{{ $json.message.content }}'.",
  "contextInstructions": [
    "Responde de forma directa, cordial y profesional sin incluir encabezados ni indicaciones de generación automática.",
    "Proporciona solo la respuesta final en formato de texto sin etiquetas."
  ]
}
```

nodo 3 simple memory=
```bash
{{ $json.context.session_id }}
```
nodo 4 http request =
```bash
url = http://evolution_api_v2:8080/message/sendText/{{$node['Edit Fields'].json.instance.name}}

Send Headers =>
apikey= {{$node['Edit Fields'].json.instance.api_key}}

Content-Type = application/json

Body Content Type=>
number = {{ $('Edit Fields').item.json.recipient.phone }}

text = {{$node['AI Agent'].json.output}}

```