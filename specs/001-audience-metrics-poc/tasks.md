---

description: "Task list template for feature implementation"
---

# Tasks: POC Pipeline de Métricas de Audiencia de TV

**Input**: Design documents from `/specs/001-audience-metrics-poc/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md (all present)

**Tests**: incluidas como tareas de primera clase (no opcionales) — la constitución del
proyecto (Principio IV, Testing Riguroso) exige cobertura pytest, tests unitarios sin
AWS, tests de integración solo contra S3 real en Actions, y un test de idempotencia
dedicado que compara contenido lógico vía DuckDB.

**Organization**: Tareas agrupadas por historia de usuario (spec.md) para permitir
implementación y prueba independiente de cada una.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Puede ejecutarse en paralelo (archivo distinto, sin dependencias pendientes)
- **[Story]**: A qué historia de usuario pertenece (US1, US2, US3)
- Cada tarea incluye la ruta de archivo exacta

## Path Conventions

Proyecto único (ver `plan.md` § Project Structure): `src/`, `tests/`, `.github/workflows/`,
`infra/` en la raíz del repositorio.

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: inicialización del proyecto y estructura base

- [X] T001 Crear la estructura de directorios del proyecto: `src/generator/`, `src/bronze/`, `src/silver/`, `src/gold/`, `tests/unit/`, `tests/integration/`, `.github/workflows/`, `infra/`, con los `__init__.py` correspondientes en `src/`
- [X] T002 [P] Crear `pyproject.toml` en la raíz declarando Python 3.12 y como únicas dependencias `boto3`, `duckdb` (runtime) y `pytest` (dev), según `plan.md` § Technical Context
- [X] T003 [P] Escribir `infra/bootstrap.sh`: script AWS CLI de un solo uso que crea (verificando existencia antes de crear, para poder re-ejecutarse sin error) el IAM Identity Provider de GitHub, el IAM Role con trust policy restringida al repo `tv-audience-metrics-pipeline` y permisos acotados a `s3:GetObject`/`PutObject`/`DeleteObject` sobre el ARN del bucket del proyecto, y el bucket S3 con Block Public Access + SSE-S3 por defecto + bucket policy que deniega tráfico no-TLS (research.md § 7)
- [X] T004 [P] Crear `tests/conftest.py` con fixtures compartidas: directorios temporales que emulan el layout `fecha=.../canal=...` de una partición S3, generador de eventos de muestra, y un marcador `integration` que se auto-omite (skip) cuando no están presentes las variables de entorno de AWS/bucket, para que `pytest` sin argumentos nunca intente tocar AWS real

**Checkpoint**: proyecto inicializado, listo para la fase Foundational

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: infraestructura compartida que DEBE completarse antes de cualquier historia de usuario

**⚠️ CRITICAL**: ninguna historia de usuario puede empezar hasta completar esta fase

- [X] T005 [P] Definir el esquema compartido en `src/schema.py`: `dataclasses` + type hints para el evento crudo, el evento limpio y la fila de métrica gold, según `data-model.md` (columnas, tipos, campos requeridos) — sin Pydantic ni otra librería de validación (research.md § 4)
- [X] T006 [P] Implementar `src/s3_io.py`: `download_partition(bucket, prefix, local_dir)`, `upload_partition(local_dir, bucket, prefix)` con semántica delete-then-write (lista y borra los objetos existentes bajo el prefijo antes de subir los nuevos), usando `boto3` puro (research.md § 1)
- [X] T007 [P] Implementar el generador reproducible de eventos de sintonía en `src/generator/events.py`: dado `seed`, `fecha`, canales y número de hogares/panelistas, produce siempre el mismo conjunto de eventos (timestamp por minuto, canal, id_hogar_panelista, universo_total) y los escribe a un directorio local; expone un CLI (`python -m src.generator.events --seed --date --out`) acorde a `quickstart.md` paso 2 (FR-001)
- [X] T008 Test unitario de determinismo del generador en `tests/unit/test_generator.py`: misma semilla + mismos parámetros ⇒ mismo output byte-a-byte en dos ejecuciones (FR-001, quickstart.md paso 2)

**Checkpoint**: fundación lista — las historias de usuario pueden empezar

---

## Phase 3: User Story 1 - Obtener Rating% y Share% por canal y franja horaria (Priority: P1) 🎯 MVP

**Goal**: pipeline bronze → silver → gold completo que produce el reporte final de
Rating%/Share% por canal y franja horaria a partir de eventos simulados.

**Independent Test**: generar un lote de eventos con semilla fija para un rango de fechas
acotado, ejecutar el pipeline completo, y verificar que el reporte final contiene
Rating%/Share% correctos y consistentes con los eventos de entrada (spec.md, Historia 1).

### Tests for User Story 1 ⚠️

> **NOTE: escribir estos tests primero; deben fallar antes de implementar**

- [X] T009 [P] [US1] Test unitario de validación de esquema bronze en `tests/unit/test_bronze_ingest.py`: eventos válidos pasan, eventos con campo requerido nulo/tipo inválido se rechazan y se cuentan sin detener el resto del lote (FR-004, Edge Cases del spec)
- [X] T010 [P] [US1] Test unitario de deduplicación/tipado silver en `tests/unit/test_silver_clean.py`: dos eventos con la misma clave natural pero `universo_total` distinto se resuelven de forma determinista (research.md § 3); nulos se manejan según la regla documentada (FR-003, FR-015)
- [X] T011 [P] [US1] Test unitario de cálculo Rating%/Share% en `tests/unit/test_gold_metrics.py`: incluye el caso de una franja horaria sin ninguna sintonización (Share% indefinido, Rating% en 0 para todos los canales) y el caso de un canal sin audiencia en una franja concreta (aparece explícito en 0, no se omite) (FR-005, FR-006, FR-014, Edge Cases)

### Implementation for User Story 1

- [X] T012 [US1] Implementar `src/bronze/ingest.py`: valida cada evento contra `src/schema.py`, descarta y cuenta los inválidos, normaliza los válidos a Parquet, escribe localmente el layout `bronze/fecha=YYYY-MM-DD/`, y sube la partición vía `src/s3_io.py` (delete-then-write) (FR-002, FR-004, FR-009)
- [X] T013 [US1] Implementar `src/silver/clean.py`: lee la partición bronze de una fecha, deduplica por clave natural (`id_hogar_panelista`, `canal`, `timestamp`) aplicando la regla determinista de `research.md` § 3, tipa y maneja nulos, escribe localmente `silver/fecha=YYYY-MM-DD/canal=<canal>/` y sube vía `src/s3_io.py` (delete-then-write) (FR-003, FR-009, FR-015)
- [X] T014 [US1] Implementar `src/gold/metrics.py`: lee la partición silver de una fecha/canal, deriva `franja_horaria` truncando `timestamp` a la hora (research.md § 6), calcula `audiencia_canal`, `audiencia_total_franja`, `rating_pct` y `share_pct` para las 24 franjas (incluyendo las de audiencia 0), escribe localmente `gold/fecha=YYYY-MM-DD/canal=<canal>/` y sube vía `src/s3_io.py` (delete-then-write) (FR-005, FR-006, FR-007, FR-009, FR-014)
- [X] T015 [US1] Test de integración bronze contra S3 real en `tests/integration/test_s3_bronze_roundtrip.py`: sube un lote de eventos de muestra, corre `ingest`, y verifica vía DuckDB el contenido de la partición `bronze/fecha=.../` en el bucket real (marcado `integration`, solo corre en Actions)
- [X] T016 [US1] Test de integración silver contra S3 real en `tests/integration/test_s3_silver_roundtrip.py`: a partir de una partición bronze ya presente en S3, corre `clean`, y verifica el contenido deduplicado de `silver/fecha=.../canal=.../` (marcado `integration`)
- [X] T017 [US1] Test de integración gold contra S3 real en `tests/integration/test_s3_gold_roundtrip.py`: a partir de una partición silver ya presente en S3, corre `metrics`, y verifica que la suma de `share_pct` por franja horaria con audiencia es 100 (SC-004) (marcado `integration`)

**Checkpoint**: Historia de Usuario 1 completamente funcional y testeable de forma
independiente — el reporte gold de Rating%/Share% se puede generar y validar de punta a
punta.

---

## Phase 4: User Story 2 - Re-ejecutar el pipeline sin duplicar ni alterar resultados (Priority: P2)

**Goal**: garantizar que re-ejecutar el pipeline completo, o solo una partición
fecha/canal, sobre el mismo input produce exactamente el mismo resultado lógico, sin
duplicados.

**Independent Test**: ejecutar el pipeline completo dos veces seguidas sobre el mismo
rango de fechas y comparar el contenido lógico de cada capa entre ambas corridas (spec.md,
Historia 2).

### Tests for User Story 2 ⚠️

- [X] T018 [P] [US2] Helper de comparación de contenido lógico en `tests/integration/_hash_helpers.py`: lee una partición vía DuckDB, ordena las filas (incluidas columnas de partición), calcula un hash agregado determinista (p. ej. `md5(string_agg(...))`) excluyendo la columna de auditoría `procesado_en`; reutilizable por tests locales e integración (research.md § 5)
- [X] T019 [US2] Test unitario de idempotencia local en `tests/unit/test_idempotency_local.py`: corre `ingest` → `clean` → `metrics` dos veces sobre el mismo lote en un directorio local, usando el helper de T018, y verifica que el hash de cada capa es idéntico entre ambas corridas y que el número de filas no aumentó

### Implementation for User Story 2

- [X] T020 [US2] Extender `src/silver/clean.py` y `src/gold/metrics.py` (implementados en US1) para aceptar un filtro opcional de canal, de forma que un reproceso de una fecha+canal específicos solo borre y reescriba esa partición puntual, sin tocar otras particiones ya existentes (spec.md, Historia 2, escenario 2; FR-009)
- [X] T021 [US2] Test de integración de idempotencia end-to-end contra S3 real en `tests/integration/test_pipeline_idempotency.py`: ejecuta el pipeline completo dos veces sobre el mismo rango de fechas contra el bucket real, compara el hash de contenido lógico (T018) de bronze/silver/gold entre ambas corridas, y verifica que el número de objetos por partición no creció (SC-002) (marcado `integration`)

**Checkpoint**: Historias 1 y 2 funcionan de forma independiente — el pipeline es seguro
de reintentar.

---

## Phase 5: User Story 3 - Ejecutar el pipeline de forma automatizada y segura en la nube (Priority: P3)

**Goal**: disparar el pipeline completo desde GitHub Actions contra el bucket S3 real,
autenticando vía OIDC (sin claves de larga duración), con la posibilidad de re-ejecutar
solo desde una etapa específica y sin exponer secretos en los logs.

**Independent Test**: disparar manualmente el workflow (sin cambios de código) y verificar
que el reporte final aparece en S3 sin que ninguna credencial de larga duración se haya
usado ni expuesto en los logs (spec.md, Historia 3).

### Tests for User Story 3 ⚠️

*(La validación de esta historia es, por naturaleza, operacional contra el entorno real de Actions — ver T027 como validación manual guiada; no aplican tests unitarios adicionales más allá de los CLIs cubiertos en T023.)*

### Implementation for User Story 3

- [X] T022 [P] [US3] Escribir `.github/workflows/pipeline.yml`: `permissions: id-token: write`, autenticación con `aws-actions/configure-aws-credentials` contra el Role creado en T003, jobs `generate` → `bronze` → `silver` → `gold` (cada uno descarga los objetos S3 relevantes, corre localmente, sube resultados), disparadores `workflow_dispatch` (inputs `start_date`, `end_date`, `start_stage`, `seed` según `contracts/workflow-dispatch-inputs.md`) y `schedule` cron
- [X] T023 [P] [US3] Añadir entrypoints CLI (`argparse`, bloque `if __name__ == "__main__"`) a `src/bronze/ingest.py`, `src/silver/clean.py` y `src/gold/metrics.py` para que cada uno pueda invocarse como un step independiente del workflow con argumentos de rango de fechas/canal
- [X] T024 [US3] Implementar el salteo de jobs según `start_stage` en `.github/workflows/pipeline.yml` (condiciones `if:` por job) de forma que las etapas anteriores a `start_stage` no se re-ejecuten (FR-012, `contracts/workflow-dispatch-inputs.md`)
- [X] T025 [US3] Añadir a los CLIs de `src/bronze/ingest.py`, `src/silver/clean.py`, `src/gold/metrics.py` un resumen de corrida en logs (fechas procesadas, particiones escritas, eventos rechazados en bronze) verificando explícitamente que ningún valor de credencial o secreto se imprime (FR-013)
- [X] T026 [US3] Ejecutar `infra/bootstrap.sh` (T003) una sola vez contra la cuenta de AWS real, y configurar `ROLE_ARN` y `BUCKET_NAME` como Secrets/Variables del repositorio en GitHub (tarea operativa, no de código)
- [X] T027 [US3] Validación manual siguiendo `quickstart.md` pasos 3–5: disparar `workflow_dispatch`, confirmar que el reporte gold aparece en el bucket real, confirmar ausencia de secretos en los logs de los 4 jobs (SC-006), y re-disparar para confirmar idempotencia end-to-end (SC-002)

**Checkpoint**: las tres historias de usuario funcionan de forma independiente — la POC es
operable de punta a punta desde GitHub Actions.

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: mejoras que afectan a más de una historia de usuario

- [X] T028 [P] Actualizar `README.md` con instrucciones de desarrollo local y un enlace a `specs/001-audience-metrics-poc/quickstart.md`
- [X] T029 [P] Revisión de PEP8 y type hints en funciones públicas de todo `src/` (constitución, Principio V)
- [X] T030 Ejecutar la guía completa de `quickstart.md` (pasos 1–5) de punta a punta y registrar el resultado como evidencia de cierre de la POC
- [X] T031 [P] Documentar en `tests/integration/README.md` que los tests marcados `integration` requieren credenciales/bucket de AWS y solo se ejecutan en GitHub Actions (se omiten localmente por defecto vía el marcador de T004)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: sin dependencias — puede empezar de inmediato
- **Foundational (Phase 2)**: depende de Setup — BLOQUEA todas las historias de usuario
- **User Story 1 (Phase 3)**: depende de Foundational
- **User Story 2 (Phase 4)**: depende de Foundational y de la implementación de US1 (T012–T014), porque extiende `silver/clean.py` y `gold/metrics.py`
- **User Story 3 (Phase 5)**: depende de Foundational y de la implementación de US1 (T012–T014), porque añade CLIs a los mismos módulos
- **Polish (Phase 6)**: depende de que las historias que se quieran entregar estén completas

### User Story Dependencies

- **US1 (P1)**: solo depende de Foundational — es el MVP
- **US2 (P2)**: depende de Foundational; reutiliza (no reimplementa) los módulos de US1, por lo que en la práctica se implementa después de US1
- **US3 (P3)**: depende de Foundational; reutiliza los módulos de US1, por lo que en la práctica se implementa después de US1 (puede hacerse en paralelo con US2, ya que tocan aspectos distintos: US2 la lógica de reproceso, US3 los CLIs y el workflow)

### Parallel Opportunities

- Setup: T002, T003, T004 en paralelo tras T001
- Foundational: T005, T006, T007 en paralelo; T008 tras T007
- US1: T009, T010, T011 (tests) en paralelo; T012 → T013 → T014 secuenciales (bronze → silver → gold); T015, T016, T017 tras su etapa correspondiente
- US2 y US3 pueden trabajarse en paralelo entre sí una vez completada US1 (tocan módulos distintos salvo T020/T023 que modifican los mismos archivos de `silver`/`gold` — coordinar si se paralelizan con más de una persona)
- Polish: T028, T029, T031 en paralelo; T030 al final

---

## Parallel Example: User Story 1

```bash
# Tests de la Historia 1 en paralelo:
Task: "Test unitario de validación de esquema bronze en tests/unit/test_bronze_ingest.py"
Task: "Test unitario de deduplicación/tipado silver en tests/unit/test_silver_clean.py"
Task: "Test unitario de cálculo Rating%/Share% en tests/unit/test_gold_metrics.py"
```

---

## Implementation Strategy

### MVP First (User Story 1 solamente)

1. Completar Phase 1: Setup
2. Completar Phase 2: Foundational (crítico — bloquea todas las historias)
3. Completar Phase 3: User Story 1
4. **PARAR y VALIDAR**: correr `tests/unit` y, en Actions, los tests de integración de US1
5. Ese es el MVP demostrable: reporte de Rating%/Share% de punta a punta

### Incremental Delivery

1. Setup + Foundational → base lista
2. US1 → validar independientemente → MVP demostrable
3. US2 → validar independientemente → pipeline seguro de reintentar
4. US3 → validar independientemente → pipeline operable desde GitHub Actions contra AWS real
5. Cada historia añade valor sin romper las anteriores

---

## Notes

- `[P]` = archivos distintos, sin dependencias pendientes entre sí
- `[Story]` mapea cada tarea a su historia de usuario para trazabilidad
- Los tests deben escribirse primero y fallar antes de implementar (T009–T011 antes de T012–T014)
- Confirmar que los tests fallan antes de implementar
- Hacer commit después de cada tarea o grupo lógico de tareas
- Parar en cualquier checkpoint para validar la historia de forma independiente
