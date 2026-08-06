# Implementation Plan: POC Pipeline de Métricas de Audiencia de TV

**Branch**: `001-audience-metrics-poc` | **Date**: 2026-08-06 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-audience-metrics-poc/spec.md`

**Note**: This template is filled in by the `/speckit-plan` command; its definition describes the execution workflow.

## Summary

Pipeline batch de tres etapas (bronze → silver → gold) que genera eventos de sintonía
simulados de forma reproducible, los normaliza a Parquet en S3, los limpia/deduplica, y
calcula Rating% y Share% por canal y franja horaria. Implementado en Python con boto3
(S3) y DuckDB (toda la transformación relacional), orquestado como jobs separados de
GitHub Actions que autentican contra AWS vía OIDC, descargan los objetos S3 relevantes al
disco efímero del runner, procesan localmente y suben los resultados — sin persistir
estado de DuckDB entre ejecuciones. Cada partición (fecha/canal) se reemplaza por
completo (delete-then-write) para garantizar idempotencia.

## Technical Context

**Language/Version**: Python 3.12

**Primary Dependencies**: boto3 (acceso a S3), duckdb (toda la transformación relacional), pytest (tests). Sin dependencias adicionales de conveniencia (sin pandas para transformaciones, sin ORMs, sin frameworks de orquestación).

**Storage**: Amazon S3 — un bucket único del proyecto con tres prefijos (`bronze/`, `silver/`, `gold/`), formato Parquet, particionado Hive (`fecha=.../canal=...`). Disco efímero del runner de GitHub Actions como área de trabajo local; ningún archivo `.duckdb` persiste entre ejecuciones.

**Testing**: pytest. Tests unitarios ejercitan la lógica de generación/transformación/agregación sobre archivos Parquet locales (sin red, sin AWS). Tests de integración ejercitan el ciclo real de subida/descarga/borrado contra el bucket S3 real y corren únicamente dentro del workflow de GitHub Actions.

**Target Platform**: Ejecución del pipeline exclusivamente en runners `ubuntu-latest` de GitHub Actions contra AWS real. El entorno local (VS Code / Linux dev container) se usa solo para desarrollo, tests unitarios y revisión de código — nunca para correr el pipeline contra el bucket real.

**Project Type**: Proyecto único — pipeline de datos batch (sin frontend, sin servicio de larga duración).

**Performance Goals**: Procesar un día completo de eventos simulados de punta a punta (generación → bronze → silver → gold) en menos de 10 minutos (SC-001).

**Constraints**: Idempotencia estricta vía delete-then-write por partición fecha/canal (FR-009); prohibido usar `random()`/`now()`/wall-clock dentro de la lógica de transformación (FR-010); sin credenciales de AWS de larga duración, solo OIDC (FR-011, FR-013); sin secrets en logs de Actions (FR-013); validación de esquema obligatoria en bronze (FR-004); PEP8 + type hints en funciones públicas (constitución, principio V).

**Scale/Scope**: Escala de POC/demo — del orden de un puñado de canales (~5–10) y unos pocos cientos de hogares/panelistas simulados, sobre un rango de fechas acotado (días, no años). No está pensado para volumen ni concurrencia de producción real.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| # | Principio | Gate | Estado |
|---|-----------|------|--------|
| I | Idempotencia y Determinismo | Escritura por partición usa delete-then-write; generador de eventos usa semilla fija (sin `random()` sin semilla); transformaciones DuckDB son SQL puro sin `now()`/wall-clock; cualquier timestamp de proceso se guarda en columna de auditoría separada de la clave de negocio. | PASS |
| II | Simplicidad y Purismo | Stack limitado a boto3 + duckdb + pytest (ver Primary Dependencies); sin ORMs, sin Dagster/Prefect/Airflow, sin pandas para transformación; estructura de módulos plana (`src/generator`, `src/bronze`, `src/silver`, `src/gold`, `src/s3_io.py`, `src/schema.py`), cada archivo legible de arriba a abajo. | PASS |
| III | Arquitectura Medallion y Datos | Tres prefijos S3 separados (bronze/silver/gold); toda transformación vía `duckdb.sql`/conexión activa; particionamiento Hive fecha/canal; cada job de Actions descarga → procesa local → sube, sin estado `.duckdb` persistente. | PASS |
| IV | Testing Riguroso | `tests/unit/` sin tocar AWS; `tests/integration/` solo contra S3 real dentro de Actions; test de idempotencia dedicado que corre el pipeline dos veces y compara hash de filas ordenadas leídas vía DuckDB (no bytes de Parquet). | PASS |
| V | Seguridad y Mínimo Privilegio | OIDC exclusivamente (sin access keys); IAM Role acotado a `s3:GetObject`/`PutObject`/`DeleteObject` sobre el ARN del bucket/prefijo del proyecto; bucket con Block Public Access; SSE-S3 por defecto en las tres capas; bucket policy deniega tráfico no-TLS; sin impresión de secrets en logs de Actions; validación de esquema en bronze (FR-004); PEP8 + type hints. | PASS |
| VI | Control de Versiones y Orquestación | Repo GitHub con PRs, `main` protegida; `.github/workflows/pipeline.yml` separa bronze → silver → gold en jobs propios, disparable por `workflow_dispatch` (con input de etapa inicial) y por `schedule`; auth vía OIDC (Identity Provider + Role con trust policy restringida a `tv-audience-metrics-pipeline`); secrets (si los hay, p. ej. nombre de bucket) vía GitHub Secrets/Variables, nunca en YAML. | PASS |
| VII | Ejecución Exclusiva en GitHub Actions | El pipeline (generación incluida) solo se ejecuta contra AWS real desde Actions; el entorno local solo corre `tests/unit` y lógica sobre archivos locales de muestra. | PASS |

No se identificaron violaciones. La tabla de Complexity Tracking se deja vacía intencionalmente (ver esa sección).

## Project Structure

### Documentation (this feature)

```text
specs/001-audience-metrics-poc/
├── plan.md              # This file (/speckit-plan command output)
├── research.md          # Phase 0 output (/speckit-plan command)
├── data-model.md        # Phase 1 output (/speckit-plan command)
├── quickstart.md        # Phase 1 output (/speckit-plan command)
├── contracts/           # Phase 1 output (/speckit-plan command)
│   ├── s3-data-contract.md
│   └── workflow-dispatch-inputs.md
└── tasks.md             # Phase 2 output (/speckit-tasks command - NOT created by /speckit-plan)
```

### Source Code (repository root)

```text
src/
├── schema.py             # Definiciones de esquema/tipos compartidas por las 3 capas (única fuente de verdad)
├── s3_io.py               # Wrappers delgados sobre boto3: descargar prefijo/partición a disco local,
│                           # subir directorio local a un prefijo, borrar-antes-de-escribir por partición
├── generator/
│   └── events.py          # Generador reproducible de eventos de sintonía por minuto (semilla fija)
├── bronze/
│   └── ingest.py           # Sube eventos crudos, normaliza a Parquet, particiona por fecha (sin lógica de negocio)
├── silver/
│   └── clean.py             # Dedup por clave natural, tipado, manejo de nulos; particiona fecha/canal
└── gold/
    └── metrics.py            # Cálculo de Rating%/Share% por canal + franja horaria; reporte final Parquet

tests/
├── conftest.py
├── unit/
│   ├── test_generator.py
│   ├── test_bronze_ingest.py
│   ├── test_silver_clean.py
│   └── test_gold_metrics.py
└── integration/
    ├── test_s3_bronze_roundtrip.py
    ├── test_s3_silver_roundtrip.py
    ├── test_s3_gold_roundtrip.py
    └── test_pipeline_idempotency.py   # corre el pipeline 2 veces y compara hash de contenido lógico

.github/workflows/
└── pipeline.yml           # workflow_dispatch (con input de etapa inicial) + schedule; jobs
                            # generate → bronze → silver → gold, cada uno descarga/procesa/sube

infra/
└── bootstrap.sh           # Script AWS CLI de un solo uso: OIDC provider, IAM Role (trust policy
                            # restringida al repo), bucket S3 con Block Public Access + SSE-S3 +
                            # bucket policy que deniega no-TLS. No es parte del pipeline en sí.
```

**Structure Decision**: Proyecto único (no hay frontend/backend separados). `src/` se
organiza por etapa del pipeline (`generator` → `bronze` → `silver` → `gold`), más dos
módulos transversales mínimos (`schema.py`, `s3_io.py`) para no duplicar definiciones de
columnas ni lógica de I/O de S3 entre etapas — sin introducir una capa de abstracción
adicional (principio II). `tests/unit` y `tests/integration` reflejan exactamente el gate
de testing de la constitución (principio IV). `infra/bootstrap.sh` vive fuera de `src/`
porque es un script de aprovisionamiento de una sola vez, no parte del pipeline que corre
en cada ejecución.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

No hay violaciones de la constitución que requieran justificación; esta sección se deja
sin entradas.
