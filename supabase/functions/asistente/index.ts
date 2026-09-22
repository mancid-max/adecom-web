/**
 * Asistente ADECOM — responde preguntas sobre los datos del dashboard.
 *
 * Vive en Supabase Edge Functions para que la API key de Anthropic nunca llegue al navegador.
 * Solo responde a usuarios con sesión iniciada en el mismo Supabase que usa el dashboard.
 * Los datos salen del bucket privado 'bi', los mismos JSON que muestra el dashboard.
 *
 * Las reglas de negocio (qué es saldo, qué stock cuenta para cortar, qué es "en producción")
 * viven en las herramientas, NO en el prompt: así el modelo no puede inventarlas.
 */
import { createClient } from "jsr:@supabase/supabase-js@2";

const ANTHROPIC_KEY = Deno.env.get("ANTHROPIC_API_KEY") ?? "";
const MODEL = Deno.env.get("ASISTENTE_MODEL") ?? "claude-sonnet-5";
const SB_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SB_SERVICE = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";

const ORIGENES = new Set([
  "https://mancid-max.github.io",
  "http://localhost:8000",
  "http://127.0.0.1:8000",
]);
const cors = (origin: string | null) => ({
  "Access-Control-Allow-Origin": origin && ORIGENES.has(origin) ? origin : "https://mancid-max.github.io",
  "Access-Control-Allow-Headers": "authorization, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Vary": "Origin",
});

/* ── Datos ────────────────────────────────────────────────────────────────── */
const admin = createClient(SB_URL, SB_SERVICE);
const cache = new Map<string, { data: unknown; at: number }>();
const TTL_MS = 10 * 60 * 1000; // el ERP exporta 3 veces al día; 10 min es de sobra

async function datos<T = any>(archivo: string): Promise<T> {
  const hit = cache.get(archivo);
  if (hit && Date.now() - hit.at < TTL_MS) return hit.data as T;
  const { data, error } = await admin.storage.from("bi").download(archivo);
  if (error || !data) throw new Error(`No se pudo leer ${archivo}: ${error?.message ?? "sin datos"}`);
  const json = JSON.parse(await data.text());
  cache.set(archivo, { data: json, at: Date.now() });
  return json as T;
}

/* ── Reglas de negocio (verificadas contra el informe de corte del ERP) ────── */
const SUC_NOMBRE: Record<string, string> = {
  "01": "Stock Perú", "02": "Mohicano Centro", "04": "San Gerardo", "05": "Mohicano Sur",
  "10": "Outlet Kennedy", "12": "Outlet", "33": "Magic World",
};
const BODEGA_CORTE = "04"; // El informe "Artículos para corte" del ERP solo descuenta San Gerardo.

const ETAPAS: Array<[string, string]> = [
  ["p_terminacion", "Terminación"], ["p_lavanderia", "Lavandería"], ["p_limpiado", "Limpiado"],
  ["p_texterno", "Taller externo"], ["p_taller", "Costura"], ["p_corte", "Corte"],
];
/** Etapa donde está realmente una orden de corte: la más avanzada con unidades pendientes. */
function etapaDe(oc: any): string {
  for (const [campo, nombre] of ETAPAS) if ((oc[campo] ?? 0) > 0) return nombre;
  return oc.saldo > 0 ? "Sin movimiento" : "Entregada";
}
/** Unidades cortadas que TODAVÍA no llegan a bodega. Nunca usar 'proceso': ese ya incluye lo entregado. */
const enProduccion = (ocs: any[]) => ocs.reduce((t, r) => t + Math.max(0, r.saldo ?? 0), 0);

const normRut = (r: string) => String(r ?? "").replace(/[^0-9kK]/g, "").toUpperCase().replace(/^0+/, "");
const sinTildes = (t: string) =>
  String(t ?? "").normalize("NFD").replace(/[̀-ͯ]/g, "").toUpperCase();
const plata = (n: number) => "$" + Math.round(n).toLocaleString("es-CL");

/** Artículo = prefijo(2) + temporada(2) + modelo(2) + color(2). Acepta "4459", "4459-00" o "01445900". */
function aArticulos(codigo: string, temp = "44"): { modelo: string; art: string | null } {
  const d = String(codigo ?? "").replace(/\D/g, "");
  if (d.length >= 8) return { modelo: d.slice(2, 6), art: d.slice(0, 8) };
  if (d.length === 6) return { modelo: d.slice(0, 4), art: "01" + d };
  if (d.length === 4) return { modelo: d, art: null };
  return { modelo: d, art: null };
}

/* ── Herramientas ─────────────────────────────────────────────────────────── */
const TOOLS = [
  {
    name: "resumen_temporada",
    description:
      "Panorama de una temporada: cuánto se pidió, se despachó y queda pendiente, cuántos clientes, y cuántas unidades falta cortar. Úsala para preguntas generales como '¿cómo va la 44?'.",
    input_schema: {
      type: "object",
      properties: { temporada: { type: "string", description: "Dos dígitos, por ejemplo 44. Por defecto 44." } },
    },
  },
  {
    name: "articulos_por_cortar",
    description:
      "Lista los artículos de una temporada donde falta cortar, o donde sobra para ofrecer. Usa la misma fórmula del informe de corte del ERP.",
    input_schema: {
      type: "object",
      properties: {
        temporada: { type: "string" },
        estado: { type: "string", enum: ["cortar", "ofrecer", "todos"], description: "Por defecto 'cortar'." },
        limite: { type: "integer", description: "Cuántos devolver. Por defecto 15." },
      },
    },
  },
  {
    name: "articulo",
    description:
      "Ficha completa de un artículo o modelo: pedido, despachado, saldo, stock por local y por talla, órdenes de corte con su etapa, y cuánto falta cortar. Acepta 4459, 4459-00 o 01445900.",
    input_schema: {
      type: "object",
      properties: { codigo: { type: "string" }, temporada: { type: "string" } },
      required: ["codigo"],
    },
  },
  {
    name: "cliente",
    description:
      "Ficha de un cliente por nombre o RUT: sus pedidos, lo que le falta despachar, deuda, cupo, tipo de cliente, cajas armadas y qué artículos le faltan.",
    input_schema: {
      type: "object",
      properties: {
        texto: { type: "string", description: "Parte del nombre o el RUT." },
        temporada: { type: "string" },
      },
      required: ["texto"],
    },
  },
  {
    name: "cajas_en_bodega",
    description:
      "Cajas ya armadas esperando despacho: cuántas, de quién, hace cuántos días y por qué monto. Sirve para saber qué está listo para salir.",
    input_schema: {
      type: "object",
      properties: {
        dias_minimo: { type: "integer", description: "Solo cajas con al menos N días esperando." },
        cliente: { type: "string" },
        limite: { type: "integer" },
      },
    },
  },
  {
    name: "clientes_sin_comprar",
    description:
      "Clientes que compraron en temporadas anteriores pero no en la actual. Para recuperar clientes.",
    input_schema: {
      type: "object",
      properties: {
        temporada: { type: "string", description: "La que NO compraron. Por defecto 44." },
        limite: { type: "integer" },
      },
    },
  },
  {
    name: "ventas",
    description:
      "Facturación real del año: total, por cliente o por período. Descuenta notas de crédito. Úsala para '¿cuánto vendí hoy?' o 'top clientes'.",
    input_schema: {
      type: "object",
      properties: {
        desde: { type: "string", description: "Fecha AAAA-MM-DD." },
        hasta: { type: "string", description: "Fecha AAAA-MM-DD." },
        cliente: { type: "string" },
        top: { type: "integer", description: "Devuelve el ranking de los N mejores clientes del período." },
      },
    },
  },
  {
    name: "estado_resultado",
    description: "Estado de resultado del contador: ingresos, costos, margen, gastos y utilidad del mes y del año.",
    input_schema: { type: "object", properties: {} },
  },
];

async function ejecutar(nombre: string, input: any): Promise<unknown> {
  const temp = String(input?.temporada ?? "44").replace(/\D/g, "").slice(0, 2) || "44";

  if (nombre === "resumen_temporada") {
    const [peds, pa, ft, sb] = await Promise.all([
      datos<any[]>("pedidos.json"), datos<any[]>("pedidos_art.json"),
      datos<any[]>("full_table.json"), datos<any[]>("saldos_bodega.json"),
    ]);
    const conTemp = peds.filter((p) => (p.u_temp?.[temp] ?? 0) > 0);
    const pedido = conTemp.reduce((t, p) => t + (p.u_temp[temp] ?? 0), 0);
    const desp = conTemp.reduce((t, p) => t + (p.u_desp?.[temp] ?? 0), 0);
    const saldo = conTemp.reduce((t, p) => t + (p.u_sal?.[temp] ?? 0), 0);
    const valorSaldo = conTemp.reduce((t, p) => t + (p.valor_sal ?? 0), 0);

    const ocs = new Map<string, any[]>();
    for (const r of ft) {
      const a = String(r.articulo ?? "").trim();
      if (!ocs.has(a)) ocs.set(a, []);
      ocs.get(a)!.push(r);
    }
    const stock = new Map(sb.map((r) => [r.art, r.suc?.[BODEGA_CORTE] ?? 0]));
    let faltaCortar = 0, sobra = 0, nCortar = 0, nOfrecer = 0;
    for (const it of pa.filter((x) => x.temp === temp)) {
      for (const v of it.modelos ?? []) {
        const art = "01" + temp + it.base.slice(-2) + String(v.mod).padStart(2, "0");
        const d = (stock.get(art) ?? 0) + enProduccion(ocs.get(art) ?? []) - (v.sal ?? v.qty ?? 0);
        if (d < 0) { faltaCortar += -d; nCortar++; } else if (d > 0) { sobra += d; nOfrecer++; }
      }
    }
    const clientes = new Set(conTemp.map((p) => normRut(p.rut) || sinTildes(p.nombre)));
    return {
      temporada: "T" + temp, pedido, despachado: desp, saldo_por_despachar: saldo,
      avance_pct: pedido ? +(desp / pedido * 100).toFixed(1) : 0,
      valor_saldo_neto: plata(valorSaldo),
      clientes: clientes.size, pedidos: conTemp.length,
      produccion: {
        unidades_por_cortar: faltaCortar, articulos_por_cortar: nCortar,
        unidades_de_sobra_para_ofrecer: sobra, articulos_con_sobra: nOfrecer,
      },
    };
  }

  if (nombre === "articulos_por_cortar") {
    const estado = String(input?.estado ?? "cortar");
    const limite = Math.min(Number(input?.limite) || 15, 60);
    const [pa, ft, sb, ex] = await Promise.all([
      datos<any[]>("pedidos_art.json"), datos<any[]>("full_table.json"),
      datos<any[]>("saldos_bodega.json"), datos<Record<string, any>>("pvc_ex.json"),
    ]);
    const ocs = new Map<string, any[]>();
    for (const r of ft) {
      const a = String(r.articulo ?? "").trim();
      if (!ocs.has(a)) ocs.set(a, []);
      ocs.get(a)!.push(r);
    }
    const stock = new Map(sb.map((r) => [r.art, r.suc?.[BODEGA_CORTE] ?? 0]));
    const filas: any[] = [];
    for (const it of pa.filter((x) => x.temp === temp)) {
      for (const v of it.modelos ?? []) {
        const mod = String(v.mod).padStart(2, "0");
        const art = "01" + temp + it.base.slice(-2) + mod;
        const sal = v.sal ?? v.qty ?? 0;
        const stk = stock.get(art) ?? 0;
        const prod = enProduccion(ocs.get(art) ?? []);
        const dif = stk + prod - sal;
        filas.push({
          articulo: `${it.base}-${mod}`, codigo: art, pedido: v.qty ?? 0, despachado: v.desp ?? 0,
          saldo_por_entregar: sal, stock_san_gerardo: stk, en_produccion: prod,
          diferencia: dif, a_cortar: dif < 0 ? -dif : 0,
          modelo_ex: ex?.[it.base]?.ex_base ?? null,
        });
      }
    }
    const sel = estado === "todos" ? filas
      : estado === "ofrecer" ? filas.filter((f) => f.diferencia > 0)
      : filas.filter((f) => f.diferencia < 0);
    sel.sort((a, b) => estado === "ofrecer" ? b.diferencia - a.diferencia : a.diferencia - b.diferencia);
    return {
      regla: "a cortar = saldo por entregar − stock en San Gerardo − lo que sigue en producción",
      total_articulos: sel.length,
      total_unidades: sel.reduce((t, f) => t + (estado === "ofrecer" ? f.diferencia : f.a_cortar), 0),
      articulos: sel.slice(0, limite),
    };
  }

  if (nombre === "articulo") {
    const { modelo, art } = aArticulos(String(input?.codigo ?? ""), temp);
    const [pa, ft, sb, ex] = await Promise.all([
      datos<any[]>("pedidos_art.json"), datos<any[]>("full_table.json"),
      datos<any[]>("saldos_bodega.json"), datos<Record<string, any>>("pvc_ex.json"),
    ]);
    const item = pa.find((x) => x.temp === temp && x.base === modelo);
    if (!item && !art) return { error: `No encontré el modelo ${modelo} en la temporada T${temp}.` };

    const colores = item?.modelos ?? [];
    const objetivo = art ? colores.filter((v: any) => "01" + temp + modelo.slice(-2) + String(v.mod).padStart(2, "0") === art) : colores;
    const detalle = (objetivo.length ? objetivo : colores).map((v: any) => {
      const mod = String(v.mod).padStart(2, "0");
      const cod = "01" + temp + modelo.slice(-2) + mod;
      const bod = sb.find((r) => r.art === cod);
      const misOcs = ft.filter((r) => String(r.articulo ?? "").trim() === cod);
      const sal = v.sal ?? v.qty ?? 0;
      const stk = bod?.suc?.[BODEGA_CORTE] ?? 0;
      const prod = enProduccion(misOcs);
      return {
        articulo: `${modelo}-${mod}`, codigo: cod,
        pedido: v.qty ?? 0, despachado: v.desp ?? 0, saldo_por_entregar: sal,
        stock_san_gerardo: stk,
        stock_otros_locales: Object.entries(bod?.suc ?? {})
          .filter(([s]) => s !== BODEGA_CORTE)
          .map(([s, u]) => `${SUC_NOMBRE[s] ?? s}: ${u}`),
        comprometido_en_cajas: bod?.cajas_total ?? 0,
        disponible_para_encajar: bod?.saldo ?? 0,
        stock_por_talla: bod?.saldo_talla ?? {},
        en_produccion: prod,
        ordenes_de_corte: misOcs.map((r) => ({
          oc: r.corte, fecha: r.fecha, programado: r.programa, cortado: r.proceso,
          entregado_a_bodega: r.bodega, falta_llegar: r.saldo, etapa: etapaDe(r), tipo: r.tipo,
        })),
        a_cortar: Math.max(0, sal - stk - prod),
        sobra_para_ofrecer: Math.max(0, stk + prod - sal),
      };
    });
    return {
      modelo, temporada: "T" + temp, bota: item?.bota || null, tiro: item?.tiro || null,
      modelo_ex_temporada_anterior: ex?.[modelo]?.ex_base ?? null,
      saldo_ex: ex?.[modelo]?.ex_saldo ?? null,
      colores: detalle,
    };
  }

  if (nombre === "cliente") {
    const q = sinTildes(String(input?.texto ?? "").trim());
    const qRut = normRut(String(input?.texto ?? ""));
    const [peds, cj, ft, sb] = await Promise.all([
      datos<any[]>("pedidos.json"), datos<any>("cajas.json"),
      datos<any[]>("full_table.json"), datos<any[]>("saldos_bodega.json"),
    ]);
    const cajas = cj?.cajas ?? [];
    const mios = peds.filter((p) =>
      (q.length >= 3 && sinTildes(p.nombre).includes(q)) || (qRut.length >= 6 && normRut(p.rut) === qRut));
    if (!mios.length) return { error: `No encontré ningún cliente que calce con "${input?.texto}".` };

    // Un mismo nombre puede tener varios RUT en el ERP: se agrupa por RUT.
    const grupos = new Map<string, any>();
    for (const p of mios) {
      const k = normRut(p.rut) || sinTildes(p.nombre);
      if (!grupos.has(k)) grupos.set(k, { nombre: p.nombre, rut: p.rut, ciudad: p.ciudad, vendedor: p.vendedor, cli: p.cli, pedidos: [] });
      grupos.get(k)!.pedidos.push(p);
    }
    const stock = new Map(sb.map((r) => [r.art, r.suc?.[BODEGA_CORTE] ?? 0]));
    const ocs = new Map<string, any[]>();
    for (const r of ft) {
      const a = String(r.articulo ?? "").trim();
      if (!ocs.has(a)) ocs.set(a, []);
      ocs.get(a)!.push(r);
    }
    return [...grupos.values()].map((g) => {
      const t = g.pedidos.filter((p: any) => (p.u_temp?.[temp] ?? 0) > 0);
      const misCajas = cajas.filter((c: any) => g.pedidos.some((p: any) => p.pedido === c.pedido) && c.estado === "bodega");
      const enCaja: Record<string, number> = {};
      for (const c of misCajas) for (const l of c.lineas ?? []) enCaja[l.art] = (enCaja[l.art] ?? 0) + l.cant;
      const faltan = t.flatMap((p: any) => (p.lineas ?? []).filter((l: any) => l.temp === temp && l.sal > 0))
        .reduce((acc: any[], l: any) => {
          const y = acc.find((x) => x.codigo === l.art);
          if (y) y.falta += l.sal; else acc.push({ codigo: l.art, falta: l.sal });
          return acc;
        }, [])
        .map((x: any) => ({
          ...x,
          ya_en_caja: enCaja[x.codigo] ?? 0,
          falta_por_encajar: Math.max(0, x.falta - (enCaja[x.codigo] ?? 0)),
          hay_en_bodega: stock.get(x.codigo) ?? 0,
          en_produccion: enProduccion(ocs.get(x.codigo) ?? []),
        }))
        .sort((a: any, b: any) => b.falta - a.falta);
      const cli = g.cli ?? null;
      return {
        nombre: g.nombre, rut: g.rut, ciudad: g.ciudad, vendedor: g.vendedor,
        tipo_de_cliente: cli?.tipo ?? null,
        credito: cli ? plata(cli.credito) : null,
        deuda: cli ? plata(cli.deuda) : null,
        cheques: cli?.cheques ? plata(cli.cheques) : null,
        cupo_disponible: cli ? plata(cli.disponible) : null,
        forma_de_pago: cli?.fpago ?? null,
        temporada: "T" + temp,
        pedido: t.reduce((s: number, p: any) => s + (p.u_temp[temp] ?? 0), 0),
        despachado: t.reduce((s: number, p: any) => s + (p.u_desp?.[temp] ?? 0), 0),
        saldo: t.reduce((s: number, p: any) => s + (p.u_sal?.[temp] ?? 0), 0),
        pedidos: t.map((p: any) => ({
          numero: p.pedido, fecha: p.fecha, dias: p.dias,
          pedido: p.u_temp[temp], despachado: p.u_desp?.[temp] ?? 0, saldo: p.u_sal?.[temp] ?? 0,
          bloqueado: !!p.bloqueo,
        })),
        cajas_armadas: misCajas.map((c: any) => ({ caja: c.caja, fecha: c.fecha, dias: c.dias, prendas: c.prendas, valor: plata(c.valor) })),
        articulos_pendientes: faltan.slice(0, 25),
      };
    });
  }

  if (nombre === "cajas_en_bodega") {
    const cj = await datos<any>("cajas.json");
    const minD = Number(input?.dias_minimo) || 0;
    const quien = sinTildes(String(input?.cliente ?? ""));
    const limite = Math.min(Number(input?.limite) || 20, 60);
    let cajas = (cj?.cajas ?? []).filter((c: any) => c.estado === "bodega" && c.dias >= minD);
    if (quien.length >= 3) cajas = cajas.filter((c: any) => sinTildes(c.cliente).includes(quien));
    cajas.sort((a: any, b: any) => b.dias - a.dias);
    return {
      cajas: cajas.length,
      prendas: cajas.reduce((t: number, c: any) => t + c.prendas, 0),
      valor_neto: plata(cajas.reduce((t: number, c: any) => t + c.valor, 0)),
      mas_de_30_dias: cajas.filter((c: any) => c.dias > 30).length,
      detalle: cajas.slice(0, limite).map((c: any) => ({
        caja: String(c.caja).replace(/^0+/, ""), fecha: c.fecha, dias: c.dias,
        cliente: c.cliente, ciudad: c.ciudad, pedido: c.pedido,
        prendas: c.prendas, valor: plata(c.valor), temporadas: c.temps,
      })),
    };
  }

  if (nombre === "clientes_sin_comprar") {
    const peds = await datos<any[]>("pedidos.json");
    const limite = Math.min(Number(input?.limite) || 30, 200);
    const porCliente = new Map<string, any>();
    for (const p of peds) {
      const k = normRut(p.rut) || sinTildes(p.nombre);
      if (!porCliente.has(k)) porCliente.set(k, { nombre: p.nombre, rut: p.rut, ciudad: p.ciudad, vendedor: p.vendedor, temps: new Map<string, number>(), ultima: "", cli: null });
      const g = porCliente.get(k)!;
      if (!g.cli && p.cli) g.cli = p.cli;
      if ((p.fecha_iso ?? "") > g.ultima) g.ultima = p.fecha_iso ?? "";
      for (const [t, u] of Object.entries(p.u_temp ?? {})) {
        if ((u as number) > 0) g.temps.set(t, (g.temps.get(t) ?? 0) + (u as number));
      }
    }
    const previas = ["40", "41", "42", "43"].filter((t) => t !== temp);
    const sin = [...porCliente.values()]
      .filter((g) => previas.some((t) => g.temps.has(t)) && !g.temps.has(temp))
      .map((g) => ({
        nombre: g.nombre, rut: g.rut, ciudad: g.ciudad, vendedor: g.vendedor,
        tipo_de_cliente: g.cli?.tipo ?? null,
        ultima_compra: g.ultima,
        unidades_por_temporada: Object.fromEntries([...g.temps.entries()].sort()),
        total_unidades: [...g.temps.values()].reduce((a, b) => a + b, 0),
      }))
      .sort((a, b) => b.total_unidades - a.total_unidades);
    return {
      temporada_no_comprada: "T" + temp,
      total: sin.length,
      unidades_que_compraban: sin.reduce((t, c) => t + c.total_unidades, 0),
      clientes: sin.slice(0, limite),
    };
  }

  if (nombre === "ventas") {
    const dv = await datos<any[]>("docs_venta.json");
    const desde = String(input?.desde ?? ""), hasta = String(input?.hasta ?? "");
    const quien = sinTildes(String(input?.cliente ?? ""));
    let docs = dv;
    if (desde) docs = docs.filter((d) => (d.fecha_iso ?? "") >= desde);
    if (hasta) docs = docs.filter((d) => (d.fecha_iso ?? "") <= hasta);
    if (quien.length >= 3) docs = docs.filter((d) => sinTildes(d.razon).includes(quien));
    // Las notas de crédito YA vienen con monto y prendas en negativo desde el ERP:
    // se suman tal cual. Invertirles el signo las convertiría en venta (inflaba el total un 23%).
    const neto = docs.reduce((t, d) => t + (d.neto ?? 0), 0);
    const prendas = docs.reduce((t, d) => t + (d.prendas ?? 0), 0);
    const base: any = {
      periodo: { desde: desde || "inicio del año", hasta: hasta || "hoy" },
      documentos: docs.length, neto: plata(neto), prendas,
      ultima_fecha_con_ventas: docs.reduce((m, d) => (d.fecha_iso > m ? d.fecha_iso : m), ""),
    };
    const top = Number(input?.top) || 0;
    if (top > 0) {
      const porCli = new Map<string, any>();
      for (const d of docs) {
        const k = normRut(d.rut);
        if (!porCli.has(k)) porCli.set(k, { cliente: d.razon, neto: 0, prendas: 0, docs: 0 });
        const g = porCli.get(k)!;
        g.neto += d.neto ?? 0; g.prendas += d.prendas ?? 0; g.docs++;
      }
      base.ranking = [...porCli.values()].sort((a, b) => b.neto - a.neto).slice(0, Math.min(top, 50))
        .map((g, i) => ({ puesto: i + 1, cliente: g.cliente, neto: plata(g.neto), prendas: g.prendas, documentos: g.docs, pct_del_total: neto ? +(g.neto / neto * 100).toFixed(1) : 0 }));
    }
    return base;
  }

  if (nombre === "estado_resultado") {
    const er = await datos<any>("estado_resultado.json");
    const secs = er?.secciones ?? [];
    const g = (k: string) => secs.find((s: any) => s.linea === k) ?? { mes: 0, acum: 0, nombre: "" };
    const l = (k: string) => {
      const s = g(k);
      return { nombre: s.nombre, mes: plata(s.mes), acumulado: plata(s.acum), pct_mes: s.pct_mes, pct_acumulado: s.pct_acum };
    };
    return {
      periodo: er?.periodo, informe_del: er?.archivo_fecha,
      ingresos: l("01"), costo_explotacion: l("02"), margen: l("03"),
      gastos_administracion: l("04"), gastos_ventas: l("05"), depreciaciones: l("07"),
      resultado_operacional: l("09"), resultado_no_operacional: l("15"),
      utilidad_del_ejercicio: l("20"),
    };
  }

  return { error: `Herramienta desconocida: ${nombre}` };
}

/* ── Modelo ───────────────────────────────────────────────────────────────── */
/** Hoy en Chile. Sin esto el modelo asume el año de su entrenamiento y busca en fechas que no existen. */
function hoyEnChile(): string {
  const f = new Intl.DateTimeFormat("es-CL", {
    timeZone: "America/Santiago", weekday: "long", day: "numeric", month: "long", year: "numeric",
  }).format(new Date());
  const iso = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Santiago", year: "numeric", month: "2-digit", day: "2-digit",
  }).format(new Date());
  return `${f} (${iso})`;
}

const SISTEMA = `Eres el asistente de ADECOM, el sistema de Mohicano Jeans, una fábrica de jeans de mujer en Chile que vende al por mayor.
Le respondes a Manu y a su equipo sobre producción, pedidos, despachos, stock y ventas.

Cómo hablas:
- Español de Chile, directo y breve. Nada de rodeos ni de repetir la pregunta.
- Responde con el número primero y la explicación después, en una o dos frases.
- Si la respuesta tiene varias filas, usa una lista corta o una tabla simple.
- Nunca menciones archivos, herramientas ni de dónde salen los datos. Habla del negocio.

Vocabulario del negocio:
- Temporada o colección: T44 es la actual, "Dolce Vita". Antes vienen T43, T42, T41, T40.
- Artículo: 8 dígitos = prefijo 01 + temporada + modelo + color. El 01445900 es el modelo 4459 color 00.
- Saldo: lo que falta despachar de un pedido, o sea lo pedido menos lo despachado.
- A cortar: lo que hay que producir. Es el saldo menos el stock de San Gerardo menos lo que ya viene en camino.
- En producción: prendas ya cortadas que todavía no llegan a bodega.
- Caja armada: pedido ya empacado en bodega, esperando despacho.
- EX: el modelo equivalente de la temporada anterior.

Reglas:
- Usa SIEMPRE las herramientas para cualquier dato. Nunca inventes ni estimes cifras.
- Si una herramienta no trae el dato, dilo claramente en vez de suponer.
- Cuando la pregunta no diga temporada, asume la T44.
- Si te piden algo que ninguna herramienta cubre, dilo y sugiere qué sí puedes responder.
- Si la pregunta es amplia ("¿cómo va la 44?"), parte con los tres o cuatro números que importan, no con uno solo.

Fechas:
- HOY es {{HOY}}. Cuando digan "hoy", "ayer", "esta semana" o una fecha sin año, calcúlalo desde ahí.
- Nunca supongas otro año: el año en curso es el de la fecha de arriba.`;

async function preguntarAClaude(pregunta: string, historial: any[]): Promise<{ answer: string; pasos: string[] }> {
  const mensajes: any[] = [...historial, { role: "user", content: pregunta }];
  const pasos: string[] = [];

  for (let vuelta = 0; vuelta < 8; vuelta++) {
    const r = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "x-api-key": ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model: MODEL, max_tokens: 2000, system: SISTEMA.replace("{{HOY}}", hoyEnChile()),
        tools: TOOLS, messages: mensajes,
      }),
    });
    if (!r.ok) throw new Error(`Anthropic ${r.status}: ${(await r.text()).slice(0, 300)}`);
    const data = await r.json();

    if (data.stop_reason === "tool_use") {
      mensajes.push({ role: "assistant", content: data.content });
      const resultados = [];
      for (const bloque of data.content) {
        if (bloque.type !== "tool_use") continue;
        pasos.push(bloque.name);
        let salida: unknown;
        try { salida = await ejecutar(bloque.name, bloque.input); }
        catch (e) { salida = { error: String((e as Error).message ?? e) }; }
        resultados.push({ type: "tool_result", tool_use_id: bloque.id, content: JSON.stringify(salida) });
      }
      mensajes.push({ role: "user", content: resultados });
      continue;
    }
    const texto = (data.content ?? []).filter((b: any) => b.type === "text").map((b: any) => b.text).join("\n").trim();
    return { answer: texto || "No pude armar una respuesta.", pasos };
  }
  return { answer: "La consulta dio demasiadas vueltas. Prueba preguntando algo más específico.", pasos };
}

/* ── Entrada ──────────────────────────────────────────────────────────────── */
Deno.serve(async (req) => {
  const origin = req.headers.get("origin");
  const cabeceras = { ...cors(origin), "content-type": "application/json" };
  if (req.method === "OPTIONS") return new Response("ok", { headers: cors(origin) });
  if (req.method !== "POST") return new Response(JSON.stringify({ error: "Usa POST." }), { status: 405, headers: cabeceras });

  try {
    if (!ANTHROPIC_KEY) throw new Error("Falta configurar ANTHROPIC_API_KEY.");

    // Solo usuarios con sesión en el dashboard.
    const token = (req.headers.get("authorization") ?? "").replace(/^Bearer\s+/i, "");
    if (!token) return new Response(JSON.stringify({ error: "Inicia sesión para usar el asistente." }), { status: 401, headers: cabeceras });
    const { data: { user }, error: authErr } = await admin.auth.getUser(token);
    if (authErr || !user) return new Response(JSON.stringify({ error: "Tu sesión expiró. Vuelve a entrar." }), { status: 401, headers: cabeceras });

    const body = await req.json().catch(() => ({}));
    const pregunta = String(body?.pregunta ?? "").trim();
    if (!pregunta) return new Response(JSON.stringify({ error: "Escribe una pregunta." }), { status: 400, headers: cabeceras });
    if (pregunta.length > 1000) return new Response(JSON.stringify({ error: "La pregunta es muy larga." }), { status: 400, headers: cabeceras });

    // Historial que manda el dashboard: se recorta para acotar el costo.
    const historial = Array.isArray(body?.historial)
      ? body.historial.filter((m: any) => (m?.role === "user" || m?.role === "assistant") && typeof m?.content === "string").slice(-6)
      : [];

    const { answer, pasos } = await preguntarAClaude(pregunta, historial);
    return new Response(JSON.stringify({ answer, pasos }), { headers: cabeceras });
  } catch (e) {
    console.error("asistente:", e);
    return new Response(JSON.stringify({ error: "No pude responder ahora. " + String((e as Error).message ?? e).slice(0, 200) }), { status: 500, headers: cabeceras });
  }
});
