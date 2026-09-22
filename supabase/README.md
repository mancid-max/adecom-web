# Asistente ADECOM — cómo publicarlo

El asistente responde preguntas sobre los datos del dashboard. Vive en Supabase para que la
clave de Anthropic nunca llegue al navegador, y solo atiende a usuarios con sesión iniciada.

```
Dashboard  ──sesión──>  Función "asistente"  ──>  Claude
(GitHub Pages)          (Supabase)               (con herramientas)
                             │
                             └──> bucket privado 'bi' (los mismos JSON del dashboard)
```

## Publicar (una sola vez)

Desde la carpeta del proyecto, en la terminal:

```bash
# 1. Entrar a Supabase (abre el navegador para autorizar)
npx supabase login

# 2. Enlazar este proyecto
npx supabase link --project-ref kdtydxihrflhziclgiof

# 3. Guardar la clave de Anthropic (queda en Supabase, no en el código)
npx supabase secrets set ANTHROPIC_API_KEY=sk-ant-...

# 4. Publicar
npx supabase functions deploy asistente
```

Listo. En el dashboard aparece un botón redondo abajo a la derecha.

## Probar sin el dashboard

```bash
npx supabase functions invoke asistente --body '{"pregunta":"¿Cómo va la temporada 44?"}'
```

## Ajustes

| Variable | Para qué | Por defecto |
|---|---|---|
| `ANTHROPIC_API_KEY` | Clave del modelo. Obligatoria. | — |
| `ASISTENTE_MODEL` | Qué modelo usar. | `claude-sonnet-5` |

Para cambiar de modelo:

```bash
npx supabase secrets set ASISTENTE_MODEL=claude-opus-5
npx supabase functions deploy asistente
```

## Qué puede responder

Ocho herramientas, cada una con las reglas del negocio ya metidas adentro:

| Herramienta | Responde |
|---|---|
| `resumen_temporada` | Cómo va una temporada: pedido, despacho, saldo, cuánto falta cortar |
| `articulos_por_cortar` | Qué artículos falta cortar y cuáles sobran para ofrecer |
| `articulo` | Ficha de un modelo: stock por local y talla, órdenes de corte, cuánto cortar |
| `cliente` | Pedidos, saldo, deuda, cupo, tipo, cajas y qué artículos le faltan |
| `cajas_en_bodega` | Cajas armadas esperando despacho y hace cuánto |
| `clientes_sin_comprar` | Clientes de temporadas anteriores que no han comprado la actual |
| `ventas` | Facturación por período o cliente, y ranking de los mejores |
| `estado_resultado` | Ingresos, margen, gastos y utilidad del mes y del año |

## Reglas que el modelo NO puede cambiar

Van en el código de las herramientas, no en el texto que lee el modelo:

- **A cortar** = saldo por entregar − stock en San Gerardo − lo que sigue en producción.
  Es la fórmula del informe "Artículos para corte" del ERP. Probado: el artículo 01444000
  da 230 a cortar, igual que el informe.
- **Stock que cuenta para cortar**: solo la bodega 04 (San Gerardo). El ERP ignora los demás locales.
- **En producción**: el saldo de las órdenes de corte, nunca lo cortado. Lo cortado ya
  incluye lo que se entregó y despachó, y contarlo lleva a pedir menos corte del necesario.
- **Etapa de una orden**: la más avanzada que todavía tenga unidades pendientes.
- **Notas de crédito**: el ERP ya las exporta con monto negativo, así que se suman tal cual.
  Invertirles el signo las convertía en venta e inflaba el total un 23%.

## Seguridad

- La clave del modelo vive en Supabase. El navegador nunca la ve.
- Sin sesión iniciada la función responde 401.
- Solo acepta llamadas desde el dominio del dashboard.
- El asistente solo lee. No puede modificar datos ni escribir en el ERP.
