# CRM Mayoristas WhatsApp — Documentación

**Fecha:** Julio 2026  
**Proyecto:** Mohicano Jeans — ADECOM WEB + ManyChat

---

## Arquitectura general

```
WhatsApp (ManyChat)
    │
    ├─ Action HTTP → crm-lead.js (Netlify)
    │       │
    │       └─ Supabase: crm_clientes + crm_pipeline + crm_interacciones
    │
    └─ CRM local Flask (crm.py)
            │
            └─ Tab "WhatsApp Mayo." en Kanban (crm_kanban.html)
```

---

## Flujo ManyChat — Mayoristas

El flujo atiende a **mayoristas existentes** que contactan via WhatsApp.  
No son leads nuevos: son clientes que ya no tienen vendedor asignado.

### Ramas del flujo

| Rama | Action mensaje | Requiere equipo |
|------|---------------|-----------------|
| Novedades y Ofertas | "Solicitó novedades y ofertas" | No (automático) |
| Sin vendedor → Visita vendedor | "Solicitó visita del vendedor" | Sí |
| Sin vendedor → Showroom | "Solicitó visitar el showroom" | Sí |
| Sin vendedor → Videollamada | "Solicitó videollamada" | Sí |
| Agendar visita → Visita vendedor | "Quiere agendar visita del vendedor" | Sí |
| Agendar visita → Showroom | "Quiere agendar visita al showroom" | Sí |
| Agendar visita → Videollamada | "Quiere agendar videollamada" | Sí |
| Ayuda con la página | "Necesita ayuda con la página web" | No (automático) |
| Necesito más ayuda | "Necesita asistencia con la página" | Sí |

### Dónde poner los Actions en ManyChat

Cada rama tiene un **Action → HTTP Request** con body JSON:
```json
{
  "secret": "mohicano-crm-2026",
  "canal": "whatsapp",
  "tipo_flujo": "mayorista",
  "nombre": "{{full name}}",
  "telefono": "{{phone}}",
  "whatsapp": "{{phone}}",
  "mc_id": "{{subscriber id}}",
  "mensaje": "[TEXTO DESCRIPTIVO DE ESTA RAMA]"
}
```

URL del endpoint: `https://mohicanojeans.netlify.app/.netlify/functions/crm-lead`

---

## crm-lead.js (Netlify Function)

**Archivo:** `c:\...\Backup\PAGINA WEB\netlify\functions\crm-lead.js`

### Lógica para mayoristas

```javascript
const esMayorista = tipo_flujo === "mayorista";
const etapaInicial = (body.etapa || "").trim() || (esMayorista ? "Contactó" : "Nuevo mensaje");
```

- Si el payload incluye `etapa` → usa ese valor directamente (ej. `"Realizó pedido"`)
- Si no → `tipo_flujo === "mayorista"` → **"Contactó"** (aparece en tab WA)
- Si no → **"Nuevo mensaje"** (aparece en tab Leads)
- La misma lógica aplica para clientes existentes (no solo nuevos)

### Búsqueda de cliente existente

1. Por `mc_id` (más confiable)
2. Por username Instagram
3. Por teléfono (limpieza de número, coincidencia parcial últimos 9 dígitos)

Si no encuentra → crea `LEAD-WA-{timestamp}` en `crm_clientes`

### Tablas que escribe

| Tabla | Cuándo |
|-------|--------|
| `crm_clientes` | Solo si no existe (crea con tipo_cliente="Lead") |
| `crm_pipeline` | Siempre (etapa "Contactó" o "Nuevo mensaje") |
| `crm_interacciones` | Siempre (detalle = mensaje del Action) |

---

## crm.py — Pipeline Mayoristas WhatsApp

**Archivo:** `c:\...\APIS\ADECOM WEB\crm.py`

### Etapas (ETAPAS_WA)

```python
ETAPAS_WA = ["Contactó", "Pendiente acción", "Realizó pedido", "Resuelto"]
ETAPA_COLOR_WA = {
    "Contactó":         "#38bdf8",   # azul
    "Pendiente acción": "#f59e0b",   # ámbar
    "Realizó pedido":   "#a78bfa",   # violeta
    "Resuelto":         "#4ade80",   # verde
}
```

**"Realizó pedido"** se setea automáticamente cuando el mayorista envía un pedido desde la página web (script-v2.js dispara crm-lead con `etapa: "Realizó pedido"`).

### Identificación de contactos WA mayoristas

En la ruta `/crm`, el backend filtra de `crm_interacciones` los ruts que tienen:
```python
inter.get("metadata", {}).get("tipo_flujo") == "mayorista"
```

Esos ruts se colocan en `columnas_wa` según la etapa actual en `crm_pipeline_actual`.

### Dato `ultima_solicitud`

Se toma del campo `detalle` de la última interacción tipo `"whatsapp"` del cliente.  
Se muestra en la card del Kanban para saber qué pidió el mayorista.

---

## crm_kanban.html — Interfaz

**Archivo:** `c:\...\APIS\ADECOM WEB\templates\crm_kanban.html`

### Tabs disponibles

| Tab | ID | Descripción |
|-----|----|-------------|
| Mayoristas | `board-mayoristas` | Clientes mayoristas tradicionales (visitas, landing) |
| WhatsApp Mayo. | `board-wa` | Mayoristas que contactaron via WhatsApp |
| Instagram / Leads | `board-leads` | Leads nuevos desde Instagram/WA/formulario |

### Cards WA Mayoristas muestran

- Nombre del cliente (o ID de lead si es nuevo)
- Badge "💬 WhatsApp"
- Teléfono clickeable → tooltip → abrir WhatsApp
- `ultima_solicitud`: qué pidió en el flujo ManyChat
- Badge de tiempo con color de alerta (verde/amarillo/rojo)

### Drag & Drop

Funciona igual que los otros boards.  
La validación de etapas en el backend acepta ETAPAS_WA gracias a `TODAS_ETAPAS`.

### Modal de cambio de etapa

El modal muestra solo las etapas del tab activo:
- Tab "mayoristas" → optgroup "Mayoristas" visible
- Tab "wa" → optgroup "WhatsApp Mayoristas" visible  
- Tab "leads" → optgroup "Leads" visible

---

## Tracking: visita + pedido desde la página web

**Archivo:** `c:\...\Backup\PAGINA WEB\script-v2.js`

### 1. Visita desde WhatsApp

ManyChat envía al mayorista un link con parámetros:
```
https://mohicanojeans.netlify.app/cole-43?wa={{phone}}&mc={{subscriber id}}
```

Al cargar la página, un IIFE al final de script-v2.js detecta los params:

```javascript
(function _crmWaTracking() {
  const params  = new URLSearchParams(window.location.search);
  const waPhone = params.get("wa") || "";
  const mcId    = params.get("mc") || "";
  if (!waPhone && !mcId) return;
  // Guardar en sessionStorage para el pedido posterior
  if (waPhone) sessionStorage.setItem("crm_wa_phone", waPhone);
  if (mcId)    sessionStorage.setItem("crm_mc_id",    mcId);
  // Registrar visita en CRM
  fetch("/.netlify/functions/crm-lead", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      secret: "mohicano-crm-2026",
      canal: "whatsapp", tipo_flujo: "mayorista",
      telefono: waPhone, mc_id: mcId,
      mensaje: "Visitó la página desde WhatsApp",
      etapa: "Contactó",
    }),
  }).catch(() => {});
})();
```

### 2. Pedido desde la página

Cuando el mayorista envía el formulario (sendRequest.onclick), después de guardar en Supabase:

```javascript
const _waPhone = sessionStorage.getItem("crm_wa_phone") || cliente.client_phone || "";
const _mcId    = sessionStorage.getItem("crm_mc_id")    || "";
if (_waPhone || _mcId) {
  fetch("/.netlify/functions/crm-lead", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      secret: "mohicano-crm-2026",
      canal: "whatsapp", tipo_flujo: "mayorista",
      nombre: cliente.razon_social || "",
      telefono: _waPhone, mc_id: _mcId,
      mensaje: `Realizó un pedido en la página (${CATALOG_SOURCE})`,
      etapa: "Realizó pedido",
    }),
  }).catch(() => {});
}
```

El sessionStorage persiste durante toda la sesión del navegador, así el teléfono del link de WhatsApp sigue disponible cuando el usuario llega al formulario.

---

## Pendientes para completar el flujo

- [ ] Configurar Actions HTTP en cada rama de ManyChat con el JSON correcto
- [ ] Subir crm-lead.js actualizado a Netlify (push al repo de la función)
- [ ] Obtener URLs definitivas: "Ver lo nuevo", landing 10% descuento
- [ ] Investigar cómo vincular automáticamente leads WA con clientes mayoristas existentes (por RUT o nombre)
- [ ] Sincronizar formulario B2B PrestaShop: subir crm-sync.php y correr mode=nuevas

---

## Archivos críticos — no commitear a ADECOM WEB

```
crm.py
templates/crm_kanban.html
templates/crm_cliente.html
netlify_functions/
```

Estos archivos son locales (Flask en 127.0.0.1:5000) y no van al repositorio público de ADECOM WEB.

---

## Supabase — Tablas

| Tabla | Descripción |
|-------|-------------|
| `crm_clientes` | Clientes y leads con datos de contacto |
| `crm_pipeline` | Historial de etapas (append-only) |
| `crm_pipeline_actual` | Vista: etapa más reciente por cliente |
| `crm_interacciones` | Log de todas las interacciones |
| `crm_emails` | Emails enviados |
| `crm_tokens` | Tokens de tracking para landing pages |
