# Data Model: POC Pipeline de Métricas de Audiencia de TV

Deriva las entidades de `spec.md` § Key Entities en esquemas concretos por capa. Los
tipos son los que usará `src/schema.py` como fuente única de verdad, consumidos tanto
por la validación de bronze como por las consultas DuckDB de silver/gold.

## 1. Evento de sintonía (crudo) — capa bronze

Representa una fila de la fuente simulada, tal cual llega, sin ninguna regla de negocio
aplicada.

| Campo | Tipo | Requerido | Descripción |
|---|---|---|---|
| `timestamp` | `TIMESTAMP` (UTC, resolución de minuto) | Sí | Minuto de sintonía. Dato de negocio — nunca se deriva de la hora del sistema. |
| `canal` | `VARCHAR` | Sí | Identificador del canal sintonizado. No vacío. |
| `id_hogar_panelista` | `VARCHAR` | Sí | Identificador único del hogar/panelista. No vacío. |
| `universo_total` | `BIGINT` | Sí | Tamaño de la población de referencia vigente en ese evento. Entero positivo. |

**Partición de escritura**: `bronze/fecha=YYYY-MM-DD/` (derivado de `timestamp`, calculado
en el momento de generación/ingesta a partir del dato de negocio, no de la hora de
proceso).

**Validación (FR-004)**: un evento se rechaza (no se escribe a bronze) si falta cualquier
campo requerido, si `timestamp` no es parseable, si `canal` o `id_hogar_panelista` están
vacíos, o si `universo_total` no es un entero positivo. Los eventos rechazados se cuentan
en un resumen de la corrida (no se detiene el procesamiento del resto del lote — Edge
Cases del spec).

## 2. Evento de sintonía (limpio) — capa silver

Mismo esquema de columnas que el evento crudo, pero garantizado único por clave natural y
sin nulos.

| Campo | Tipo | Notas |
|---|---|---|
| `timestamp` | `TIMESTAMP` | Igual que en bronze. |
| `canal` | `VARCHAR` | Igual que en bronze. |
| `id_hogar_panelista` | `VARCHAR` | Igual que en bronze. |
| `universo_total` | `BIGINT` | Resuelto de forma determinista en caso de conflicto (ver `research.md` § 3). |

**Clave natural (unicidad)**: (`id_hogar_panelista`, `canal`, `timestamp`).

**Regla de deduplicación (FR-003, FR-015)**: cuando existen dos o más filas crudas con la
misma clave natural, silver conserva una sola fila, resuelta por el criterio determinista
documentado en `research.md` § 3 (nunca por orden de llegada/procesamiento).

**Partición de escritura**: `silver/fecha=YYYY-MM-DD/canal=<canal>/`.

## 3. Franja horaria (concepto derivado, no persistido como entidad propia)

No es una tabla independiente; es una expresión derivada de `timestamp` truncado a la
hora (`00`–`23`), usada como columna de agrupación al calcular gold. Ver `research.md` § 6.

## 4. Métrica de audiencia (reporte final) — capa gold

Una fila por combinación fecha + canal + franja horaria, incluyendo las franjas sin
audiencia para ese canal (FR-014).

| Campo | Tipo | Descripción |
|---|---|---|
| `fecha` | `DATE` | Fecha del reporte (columna de partición, también presente como dato). |
| `canal` | `VARCHAR` | Canal reportado (columna de partición, también presente como dato). |
| `franja_horaria` | `TINYINT` (0–23) | Hora del día a la que pertenece la fila. |
| `audiencia_canal` | `BIGINT` | Conteo de hogares/panelistas distintos sintonizando ese canal en esa franja (silver deduplicado). |
| `audiencia_total_franja` | `BIGINT` | Conteo de hogares/panelistas distintos sintonizando **cualquier** canal en esa franja (denominador de Share%). |
| `universo_total` | `BIGINT` | Universo total vigente usado como denominador de Rating% para esa fecha/franja. |
| `rating_pct` | `DOUBLE` | `audiencia_canal / universo_total * 100`. `0` si `audiencia_canal = 0`. |
| `share_pct` | `DOUBLE` | `audiencia_canal / audiencia_total_franja * 100` si `audiencia_total_franja > 0`; `NULL` (indefinido, ver Edge Cases del spec) si `audiencia_total_franja = 0`. |

**Partición de escritura**: `gold/fecha=YYYY-MM-DD/canal=<canal>/`.

**Invariante verificable (SC-004)**: para cualquier (`fecha`, `franja_horaria`) con
`audiencia_total_franja > 0`, la suma de `share_pct` de todas las filas de esa
fecha/franja es 100 (dentro de margen de redondeo).

## 5. Metadata de auditoría (no es clave de negocio)

Cada escritura (bronze/silver/gold) añade, fuera de las columnas anteriores, una columna
de auditoría separada: `procesado_en` (`TIMESTAMP`, hora de proceso). Nunca participa en
deduplicación, agregación, particionamiento ni en el hash de comparación de idempotencia
(principio I de la constitución) — se excluye explícitamente al leer para tests de
idempotencia.

## Relaciones entre capas

```text
generator/events.py
        │  (in-memory, misma semilla ⇒ mismos eventos)
        ▼
bronze (crudo, validado, particionado por fecha)
        │  dedup + tipado + manejo de nulos
        ▼
silver (limpio, único por clave natural, particionado por fecha/canal)
        │  agregación por fecha + canal + franja horaria
        ▼
gold (Rating% / Share%, particionado por fecha/canal)
```
