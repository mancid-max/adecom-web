# ADECOM a PostgreSQL: Venta y Produccion

Este documento baja el analisis de `C:\Adecom` a un modelo inicial de PostgreSQL para trabajar solo con los modulos de `venta` y `produccion`.

## Objetivo

Separar el proyecto web de los archivos BBx/PRO5 de ADECOM y mover el dominio operativo a tablas SQL legibles, consultables y sincronizables.

## Hallazgos base

- Las tablas internas de ADECOM son archivos `BBx / BASIS PRO/5`.
- Archivos como `PEDENC` y `DETVEN` comienzan con la firma `<<bbx>>`.
- No son tablas SQL ni DBF; se necesita una capa de extraccion.
- Para el foco actual, no hace falta migrar contabilidad completa ni recursos humanos.

## Archivos fuente relevantes

Ubicacion principal:
[C:\Adecom\MOHICANO\DATOS\MOHICANO](</C:/Adecom/MOHICANO/DATOS/MOHICANO>)

Venta:

- `PEDENC`
- `PEDDET`
- `ENCVEN`
- `DETVEN`
- `VENCAJA`
- `EXTVEN`

Produccion:

- `OPDETA`
- `OCORTE`
- `OPTALL`
- `SECENC`
- `SECDET`
- `SECSAL`

## Mapa ADECOM -> PostgreSQL

### Venta

`PEDENC` -> `adecom.pedidos`

- Rol: encabezado de pedido
- Confianza: media
- Campos inferidos:
  - `pedido_numero`
  - `cliente_id`
  - `vendedor_id`
  - `bodega_id`
  - `fecha_pedido`
  - `fecha_compromiso`
  - `fecha_vencimiento`
  - `condicion_pago`
  - `temporada`
  - `coleccion`
  - `estado`

`PEDDET` -> `adecom.pedido_detalle`

- Rol: detalle por articulo/talla
- Confianza: alta
- Campos inferidos:
  - `pedido_id`
  - `articulo_id`
  - `talla`
  - `cantidad_pedida`
  - `cantidad_despachada`
  - `cantidad_saldo`
  - `precio_unitario`
  - `ocorte_numero`

`ENCVEN` -> `adecom.ventas_documentos`

- Rol: encabezado de documento comercial
- Confianza: media
- Campos inferidos:
  - `tipo_documento`
  - `folio`
  - `cliente_id`
  - `vendedor_id`
  - `pedido_id`
  - `fecha_emision`
  - `fecha_vencimiento`
  - `condicion_pago`
  - `contabilizado`
  - `estado_sii`
  - `neto`
  - `iva`
  - `total`

`DETVEN` -> `adecom.ventas_detalle`

- Rol: detalle del documento
- Confianza: media-alta
- Campos inferidos:
  - `documento_id`
  - `articulo_id`
  - `talla`
  - `cantidad`
  - `precio_unitario`
  - `descuento_pct`
  - `neto_linea`
  - `iva_linea`
  - `total_linea`

`VENCAJA` -> `adecom.ventas_caja`

- Rol: pagos / caja / recaudo
- Confianza: media

`EXTVEN` -> `adecom.ventas_documentos_extra`

- Rol: extension del documento
- Estado: no modelado todavia como tabla separada en el DDL inicial
- Nota: primero conviene revisar registros reales para saber si vale la pena normalizarla

### Produccion

`OPDETA` -> `adecom.produccion_ordenes`

- Rol: orden de produccion / detalle operativo principal
- Confianza: alta
- Campos inferidos:
  - `op_numero`
  - `pedido_id`
  - `cliente_id`
  - `articulo_id`
  - `fecha_creacion`
  - `fecha_compromiso`
  - `cantidad_programada`
  - `cantidad_cortada`
  - `cantidad_terminada`
  - `estado`
  - `taller_codigo`

`OCORTE` -> `adecom.produccion_cortes`

- Rol: orden de corte
- Confianza: alta

`OPTALL` -> `adecom.produccion_tallas`

- Rol: cantidades por talla
- Confianza: alta

`SECENC` -> `adecom.produccion_secciones`

- Rol: encabezado de movimiento por seccion
- Confianza: media

`SECDET` -> `adecom.produccion_secciones_detalle`

- Rol: detalle de movimiento productivo
- Confianza: alta

`SECSAL` -> `adecom.produccion_saldos`

- Rol: saldo pendiente por seccion / articulo / talla
- Confianza: alta

## Relaciones sugeridas

- `pedidos 1:N pedido_detalle`
- `pedidos 1:N ventas_documentos`
- `ventas_documentos 1:N ventas_detalle`
- `pedidos 1:N produccion_ordenes`
- `produccion_ordenes 1:N produccion_cortes`
- `produccion_ordenes 1:N produccion_tallas`
- `produccion_ordenes 1:N produccion_secciones`
- `produccion_secciones 1:N produccion_secciones_detalle`
- `produccion_ordenes / produccion_cortes 1:N produccion_saldos`

## Pistas tecnicas encontradas en los programas

En [PRO420.APL](</C:/Adecom/MOHICANO/PRODUC/PRO420.APL>) aparecen juntos:

- `PEDDET`
- `PEDENC`
- `MAEART`
- `TALLAS.PAR`
- `OCORTE`
- `PARAME`
- `MAEBOD`
- `SECSAL`

Esto sugiere la cadena:

`pedido -> articulo -> corte -> saldo por seccion`

En [PRO404.APL](</C:/Adecom/MOHICANO/PRODUC/PRO404.APL>) aparecen:

- `SECSAL`
- `SECCION`
- `OCORTE`
- `FORMULA1`
- `MAEART`
- `MOVDET`
- `CONSUM`
- `SECDET`

Esto sugiere que `SECSAL` y `SECDET` son claves para reconstruir avance y faltantes de produccion.

En [CTE562.APL](</C:/Adecom/MOHICANO/CTACTE/CTE562.APL>) aparece:

- `LIBCOMA7.DAT`
- `ENCVEN`

Esto confirma que `ENCVEN` participa en el flujo documental y contable.

## Entregables agregados al repo

- DDL inicial: [sql/adecom_venta_produccion_schema.sql](/c:/Users/Lenovo/Desktop/Backup/Data%20Manu/APIS/ADECOM%20WEB/sql/adecom_venta_produccion_schema.sql)
- Vista visual: [docs/POSTGRESQL_VENTA_PRODUCCION_VISUAL.md](/c:/Users/Lenovo/Desktop/Backup/Data%20Manu/APIS/ADECOM%20WEB/docs/POSTGRESQL_VENTA_PRODUCCION_VISUAL.md)

## Limites de este primer modelo

- Todavia no hay extractor BBx -> SQL.
- Los nombres de columnas son inferidos y deben validarse con registros reales.
- Falta mapear tablas maestras complementarias como `MAEART`, `MAEBOD`, `SECCION`, `PARAME`.
- `EXTVEN` no esta modelada en esta primera version.

## Siguiente paso recomendado

1. Extraer una muestra real de registros de:
   - `PEDENC`
   - `PEDDET`
   - `ENCVEN`
   - `DETVEN`
   - `OPDETA`
   - `SECDET`
2. Validar claves y longitudes reales.
3. Ajustar el DDL.
4. Construir un extractor incremental hacia PostgreSQL.
