# Contrato de datos: capas S3 (bronze / silver / gold)

Esta es la interfaz pública del pipeline: cualquier consumidor (otro job, un notebook de
análisis, un test de integración) debe poder depender de esta forma sin conocer el código
interno. Los tipos de columna referencian `data-model.md`.

## Layout de prefijos

```text
s3://<bucket-del-proyecto>/
├── bronze/fecha=YYYY-MM-DD/*.parquet
├── silver/fecha=YYYY-MM-DD/canal=<canal>/*.parquet
└── gold/fecha=YYYY-MM-DD/canal=<canal>/*.parquet
```

- `fecha` usa formato `YYYY-MM-DD` (ISO 8601, sin hora).
- `canal` es el valor tal cual aparece en el evento de origen (sensible a mayúsculas,
  sin normalización adicional más allá de trim de espacios).
- Cada archivo `.parquet` dentro de una partición es autocontenible: leer todos los
  `.parquet` de una partición con `read_parquet('.../*.parquet')` en DuckDB reproduce el
  contenido completo de esa partición.

## Garantías del contrato

1. **Reemplazo atómico por partición**: antes de escribir en una partición
   (`fecha=.../` en bronze, `fecha=.../canal=.../` en silver y gold), todos los objetos
   existentes bajo ese prefijo se borran primero. Un consumidor nunca verá una mezcla de
   datos de dos corridas distintas dentro de la misma partición.
2. **Sin duplicados**: dentro de una partición, no hay dos filas con la misma clave
   natural de esa capa (ver `data-model.md`).
3. **Sin franjas faltantes en gold**: para cada canal presente en `silver` en una fecha
   dada, `gold` contiene una fila por cada una de las 24 franjas horarias del día, incluso
   con `audiencia_canal = 0`.
4. **Columna de auditoría separada**: la columna `procesado_en` (si está presente) es
   metadata de proceso y no forma parte de ninguna clave; los consumidores no deben
   usarla para lógica de negocio ni para deduplicar.
5. **Esquema estable**: los nombres y tipos de columna listados en `data-model.md` no
   cambian entre ejecuciones para una misma capa; cualquier cambio de esquema es un
   cambio de contrato y debe versionarse (ver `plan.md` § Governance del proyecto, vía
   constitución).

## Ejemplo de lectura (para consumidores, con partition pruning)

```sql
SELECT canal, franja_horaria, rating_pct, share_pct
FROM read_parquet('s3://<bucket>/gold/fecha=2026-08-01/*/*.parquet', hive_partitioning = true)
WHERE fecha = DATE '2026-08-01';
```
