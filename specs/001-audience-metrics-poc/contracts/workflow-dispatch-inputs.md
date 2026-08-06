# Contrato: inputs de `workflow_dispatch` del pipeline

Interfaz con la que un operador humano dispara manualmente el pipeline (Historia de
Usuario 3 del spec). El `schedule` cron no provee estos inputs y usa los valores por
defecto documentados abajo.

## Inputs

| Input | Tipo | Requerido | Default | Descripción |
|---|---|---|---|---|
| `start_date` | `string` (`YYYY-MM-DD`) | Sí | — | Primera fecha del rango a procesar (inclusive). |
| `end_date` | `string` (`YYYY-MM-DD`) | Sí | — | Última fecha del rango a procesar (inclusive). |
| `start_stage` | `choice`: `generate` \| `bronze` \| `silver` \| `gold` | No | `generate` | Etapa desde la que arranca la corrida; las etapas anteriores no se re-ejecutan (FR-012). |
| `seed` | `string` (entero) | No | valor fijo del repo (ver `infra`/config del generador) | Semilla del generador de eventos simulados; solo aplica si `start_stage = generate`. |

## Comportamiento

- Los jobs del workflow (`generate`, `bronze`, `silver`, `gold`) se ejecutan en secuencia;
  un job se omite (`skip`) si su etapa es anterior a `start_stage`.
- Cada job que se ejecuta procesa exactamente el rango `[start_date, end_date]`, partición
  por partición, aplicando delete-then-write (FR-009) — re-disparar con el mismo rango y
  el mismo `seed` es seguro y produce el mismo resultado (Historia de Usuario 2).
- Si `start_stage` es `bronze`, `silver` o `gold`, el job asume que los datos de las
  etapas anteriores ya existen en S3 para ese rango; no valida su presencia más allá de
  fallar de forma explícita si el prefijo de entrada está vacío.

## Salida observable

- Cada job imprime, como resumen en el log de Actions (sin datos sensibles): rango de
  fechas procesado, número de particiones escritas, número de eventos rechazados por
  validación de esquema (solo relevante en el job `bronze`).
- El resultado final verificable es el contenido de `s3://<bucket>/gold/...` para el
  rango solicitado, según `s3-data-contract.md`.
