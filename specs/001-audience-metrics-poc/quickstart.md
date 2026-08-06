# Quickstart: validar la POC de métricas de audiencia

Guía de validación end-to-end. No repite código de implementación (eso vive en
`tasks.md` y en `src/`); asume que las tareas de `/speckit-tasks` ya se implementaron.

## Prerrequisitos

- Python 3.12 y las dependencias del proyecto instaladas (`boto3`, `duckdb`, `pytest`).
- Para la parte local (pasos 1–2): no se necesita ninguna credencial de AWS.
- Para la parte en la nube (pasos 3–5): una cuenta de AWS donde ya se corrió
  `infra/bootstrap.sh` una vez (crea el OIDC provider, el IAM Role restringido al repo
  `tv-audience-metrics-pipeline`, y el bucket S3 con Block Public Access + SSE-S3), y el
  ARN del Role + nombre del bucket configurados como Secrets/Variables del repositorio
  de GitHub.

## 1. Validar la lógica localmente (sin AWS)

```bash
pytest tests/unit -v
```

**Resultado esperado**: todos los tests unitarios pasan — cubren generación reproducible
de eventos, validación de esquema en bronze, deduplicación en silver, y cálculo de
Rating%/Share% en gold — sin ninguna llamada de red.

## 2. Probar el determinismo del generador a mano

```bash
python -m src.generator.events --seed 42 --date 2026-08-01 --out /tmp/eventos_run1
python -m src.generator.events --seed 42 --date 2026-08-01 --out /tmp/eventos_run2
diff -rq /tmp/eventos_run1 /tmp/eventos_run2
```

**Resultado esperado**: `diff` no reporta ninguna diferencia — misma semilla, mismos
eventos (valida FR-001 antes de tocar S3).

## 3. Ejecutar el pipeline completo en GitHub Actions

1. En GitHub → Actions → workflow del pipeline → **Run workflow**.
2. Completar `start_date` y `end_date` (por ejemplo, `2026-08-01` a `2026-08-01`), dejar
   `start_stage = generate`.
3. Esperar a que los 4 jobs (`generate`, `bronze`, `silver`, `gold`) terminen en verde.

**Resultado esperado** (según `contracts/s3-data-contract.md`):

```bash
aws s3 ls s3://<bucket>/gold/fecha=2026-08-01/ --recursive
```

muestra al menos un archivo `.parquet` por canal simulado, y ningún log de ningún job
imprime un valor de credencial (SC-006).

## 4. Validar el reporte final

```sql
-- con duckdb CLI o duckdb.sql en Python, tras descargar la partición localmente
SELECT canal, franja_horaria, rating_pct, share_pct
FROM read_parquet('gold/fecha=2026-08-01/*/*.parquet', hive_partitioning = true)
ORDER BY franja_horaria, canal;
```

**Resultado esperado**: para cada `franja_horaria` con al menos una sintonización, la
suma de `share_pct` de todos los canales es 100 (SC-004); los canales sin audiencia en
una franja aparecen con `rating_pct = 0` (FR-014), no ausentes.

## 5. Validar idempotencia end-to-end

1. Re-disparar el mismo workflow con el mismo `start_date`/`end_date` (paso 3), sin
   cambiar `seed`.
2. Comparar el contenido lógico de `gold/fecha=2026-08-01/` antes y después:

```bash
pytest tests/integration/test_pipeline_idempotency.py -v
```

**Resultado esperado**: el test pasa — el hash de contenido lógico (filas ordenadas,
leídas vía DuckDB, sin la columna `procesado_en`) es idéntico entre ambas corridas, y el
número de objetos en cada partición no aumentó (SC-002).
