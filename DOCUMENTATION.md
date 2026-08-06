# Documentación técnica: TV Audience Metrics Pipeline

POC de un pipeline de datos que calcula métricas de audiencia de TV (**Rating%** y
**Share%**) a partir de eventos simulados de sintonía, usando arquitectura Medallion
(bronze → silver → gold), DuckDB para todo el procesamiento, S3 como almacenamiento, y
GitHub Actions como único entorno de ejecución real.

Este documento es la referencia técnica consolidada del proyecto. Para el detalle de
diseño paso a paso (spec → plan → tareas), ver [`specs/001-audience-metrics-poc/`](specs/001-audience-metrics-poc/).
Para los principios no negociables del proyecto, ver [`.specify/memory/constitution.md`](.specify/memory/constitution.md).

## Índice

1. [Resumen y objetivo](#1-resumen-y-objetivo)
2. [Arquitectura](#2-arquitectura)
3. [Modelo de datos](#3-modelo-de-datos)
4. [Referencia de módulos](#4-referencia-de-módulos)
5. [Determinismo e idempotencia](#5-determinismo-e-idempotencia)
6. [Seguridad](#6-seguridad)
7. [Testing](#7-testing)
8. [CI/CD: GitHub Actions](#8-cicd-github-actions)
9. [Cómo ejecutar en local](#9-cómo-ejecutar-en-local)
10. [Cómo poner en marcha contra AWS real](#10-cómo-poner-en-marcha-contra-aws-real)
11. [Estructura de directorios](#11-estructura-de-directorios)
12. [Decisiones de diseño y alternativas descartadas](#12-decisiones-de-diseño-y-alternativas-descartadas)

---

## 1. Resumen y objetivo

Dado un conjunto de canales de TV y una población simulada de hogares/panelistas, el
pipeline:

1. **Genera** eventos de sintonía por minuto de forma reproducible (semilla fija).
2. **Ingiere** esos eventos a S3 en Parquet, validando su esquema (capa **bronze**).
3. **Limpia** y deduplica los eventos por clave natural (capa **silver**).
4. **Agrega** Rating% y Share% por canal y franja horaria, y emite el reporte final
   (capa **gold**).

El pipeline es **idempotente**: re-ejecutarlo sobre el mismo rango de fechas nunca
produce duplicados ni resultados distintos. Es **determinista**: no hay `random()` sin
semilla ni `now()`/wall-clock dentro de la lógica de transformación. Y corre
**exclusivamente en GitHub Actions** contra AWS real, autenticado vía OIDC — nunca con
credenciales de larga duración.

## 2. Arquitectura

```text
┌─────────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  generator  │ --> │  raw/    │ --> │  bronze/ │ --> │  silver/ │ --> │  gold/   │
│ (semilla)   │     │ (S3, pre-│     │ (S3,     │     │ (S3,     │     │ (S3,     │
│             │     │  bronze) │     │ validado)│     │ deduped) │     │ Rating/  │
│             │     │          │     │          │     │          │     │ Share)   │
└─────────────┘     └──────────┘     └──────────┘     └──────────┘     └──────────┘
```

- **`raw/fecha=YYYY-MM-DD/`**: prefijo de aterrizaje pre-bronze. Contiene el
  `events.jsonl` producido directamente por el generador, sin ninguna validación. No es
  una capa medallion en sí misma — es el mecanismo por el cual el job `generate` le pasa
  su salida al job `bronze` a través de S3 (cada job de Actions corre en un runner
  distinto y efímero).
- **`bronze/fecha=YYYY-MM-DD/`**: eventos validados contra el esquema y normalizados a
  Parquet. Sin lógica de negocio (sin dedup, sin cálculo de métricas).
- **`silver/fecha=YYYY-MM-DD/canal=<canal>/`**: eventos deduplicados por clave natural,
  tipados, sin nulos.
- **`gold/fecha=YYYY-MM-DD/canal=<canal>/`**: una fila por (fecha, canal, franja
  horaria) con `rating_pct` y `share_pct`, incluyendo las 24 franjas del día aunque
  tengan audiencia cero.

Cada capa vive en su propio prefijo S3, particionado en formato Hive
(`fecha=.../canal=...`) para permitir partition pruning al leer. Ver el contrato
completo en [`specs/001-audience-metrics-poc/contracts/s3-data-contract.md`](specs/001-audience-metrics-poc/contracts/s3-data-contract.md).

### Patrón de ejecución

Ningún job de GitHub Actions asume persistencia de un archivo `.duckdb` entre
ejecuciones. El patrón, repetido en `bronze`, `silver` y `gold`, es siempre:

1. Descargar del bucket S3 (vía `src/s3_io.py`) los objetos de la partición de entrada
   al disco efímero del runner.
2. Procesar completamente en local con DuckDB (`duckdb.sql(...)`, nunca la extensión
   `httpfs` para leer `s3://` directamente — ver [§12](#12-decisiones-de-diseño-y-alternativas-descartadas)).
3. Subir el resultado de vuelta a S3, reemplazando la partición de salida por completo
   (delete-then-write).

## 3. Modelo de datos

### Evento de sintonía (bronze)

| Campo | Tipo | Notas |
|---|---|---|
| `timestamp` | `TIMESTAMP` | Minuto de sintonía (dato de negocio) |
| `canal` | `VARCHAR` | No vacío |
| `id_hogar_panelista` | `VARCHAR` | No vacío |
| `universo_total` | `BIGINT` | Entero positivo |
| `procesado_en` | `TIMESTAMP` | Metadata de auditoría — nunca parte de una clave |

### Evento de sintonía (silver)

Mismas columnas que bronze, garantizado único por la clave natural
**(`id_hogar_panelista`, `canal`, `timestamp`)**. Si dos filas crudas comparten esa
clave con un `universo_total` distinto, silver conserva el de mayor valor
(`MAX(universo_total)` agrupado por la clave — una resolución determinista que no
depende del orden de lectura).

### Métrica de audiencia (gold)

| Campo | Tipo | Fórmula / notas |
|---|---|---|
| `fecha` | `DATE` | Columna de partición |
| `canal` | `VARCHAR` | Columna de partición |
| `franja_horaria` | `TINYINT` (0–23) | `EXTRACT(hour FROM timestamp)` |
| `audiencia_canal` | `BIGINT` | Hogares/panelistas distintos sintonizando ese canal en esa franja |
| `audiencia_total_franja` | `BIGINT` | Hogares/panelistas distintos sintonizando **cualquier** canal en esa franja |
| `universo_total` | `BIGINT` | `MAX(universo_total)` de la franja |
| `rating_pct` | `DOUBLE` | `audiencia_canal / universo_total * 100`; `0` si no hay audiencia |
| `share_pct` | `DOUBLE` \| `NULL` | `audiencia_canal / audiencia_total_franja * 100`; `NULL` (no `0`) si `audiencia_total_franja = 0` |
| `procesado_en` | `TIMESTAMP` | Metadata de auditoría |

**Invariante verificable**: para cualquier franja con audiencia, la suma de
`share_pct` de todos los canales es 100. Cubierto por
`tests/unit/test_gold_metrics.py` y `tests/integration/test_s3_gold_roundtrip.py`.

Detalle completo de entidades y relaciones:
[`specs/001-audience-metrics-poc/data-model.md`](specs/001-audience-metrics-poc/data-model.md).

## 4. Referencia de módulos

| Módulo | Responsabilidad | Función principal |
|---|---|---|
| `src/schema.py` | Única fuente de verdad del esquema; validación de eventos crudos | `validate_raw_event(row) -> str \| None` |
| `src/s3_io.py` | Toda la I/O de S3 del proyecto (boto3 puro) | `download_partition()`, `upload_partition()` (delete-then-write) |
| `src/generator/events.py` | Generador reproducible de eventos de sintonía | `generate_events(seed, fecha, ...)` |
| `src/bronze/ingest.py` | Validación de esquema + normalización a Parquet | `ingest_to_local(events, local_root, *, procesado_en)` |
| `src/silver/clean.py` | Deduplicación, tipado, manejo de nulos | `clean_partition(bronze_dir, silver_root, *, procesado_en, canal_filter=None)` |
| `src/gold/metrics.py` | Cálculo de Rating%/Share% | `compute_metrics(silver_date_dir, gold_root, *, procesado_en, canales=None, canal_filter=None)` |

Cada uno de `bronze/ingest.py`, `silver/clean.py` y `gold/metrics.py` expone además un
CLI (`python -m src.<paquete>.<módulo> --bucket ... --start-date ... --end-date ...`)
que es lo que invoca `.github/workflows/pipeline.yml`. El CLI se encarga de:
descargar la partición de entrada de S3, llamar a la función pura correspondiente, y
subir la partición de salida — la función pura en sí nunca toca AWS, lo que es lo que
permite testearla como test unitario.

### CLIs disponibles

```bash
# Generador (local-only si se omite --bucket)
python -m src.generator.events --seed 42 --start-date 2026-08-01 --end-date 2026-08-01 \
  --out _work/generated [--bucket <bucket>] [--canales Canal1,Canal2] \
  [--num-hogares 200] [--universo-total 100000]

# Bronze
python -m src.bronze.ingest --bucket <bucket> --start-date 2026-08-01 [--end-date 2026-08-03]

# Silver (--canal opcional: reprocesa solo un canal)
python -m src.silver.clean --bucket <bucket> --start-date 2026-08-01 [--canal Canal1]

# Gold (--canales es la lista definitiva de canales del día, para no omitir canales sin audiencia)
python -m src.gold.metrics --bucket <bucket> --start-date 2026-08-01 --canales Canal1,Canal2,Canal3
```

## 5. Determinismo e idempotencia

Dos garantías no negociables (constitución, Principio I):

- **Determinismo**: ninguna función de transformación usa `random()` sin semilla,
  `now()`, o cualquier valor de wall-clock. El generador (`src/generator/events.py`)
  usa una instancia `random.Random(seed)` consumida en un orden de iteración fijo
  (hogar → minuto). Cualquier timestamp de proceso (`procesado_en`) se captura **una
  sola vez**, en el CLI (`datetime.now(timezone.utc)`), fuera de toda función de
  transformación, y se pasa como parámetro explícito — nunca se calcula dentro de
  `ingest_to_local`, `clean_partition` ni `compute_metrics`.

- **Idempotencia**: antes de escribir cualquier partición, `src/s3_io.upload_partition`
  borra todos los objetos existentes bajo ese prefijo y luego sube los nuevos
  (delete-then-write). Además, localmente, cada archivo de salida tiene un nombre fijo
  (`part-0000.parquet`), así que re-ejecutar contra el mismo directorio local también
  sobrescribe en vez de acumular.

**Cómo se verifica**: nunca comparando bytes de Parquet (dos escrituras lógicamente
idénticas pueden diferir en metadata/orden de row groups/compresión). En cambio,
`tests/integration/_hash_helpers.py::hash_partition_content` lee la partición vía
DuckDB, excluye `procesado_en`, ordena las filas, y calcula un hash `md5` agregado. Ese
hash se compara entre dos corridas independientes en:
- `tests/unit/test_idempotency_local.py` (sin AWS, corre localmente)
- `tests/integration/test_pipeline_idempotency.py` (contra S3 real, solo en Actions)

## 6. Seguridad

- **Sin credenciales de larga duración**: GitHub Actions se autentica contra AWS vía
  OIDC (`aws-actions/configure-aws-credentials`, `permissions: id-token: write`).
  Localmente se usan variables de entorno / AWS profiles, nunca claves en código.
- **Mínimo privilegio**: el IAM Role creado por `infra/bootstrap.sh` solo tiene
  `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` (scoped al ARN de objetos del
  bucket) y `s3:ListBucket` (scoped al ARN del bucket, necesario para saber qué borrar
  antes de escribir). Nunca `s3:*` ni `Resource: "*"`.
- **Trust policy restringida**: el Role solo puede ser asumido desde tokens OIDC cuyo
  `sub` sea `repo:<GITHUB_ORG>/tv-audience-metrics-pipeline:*`.
- **Bucket privado**: Block Public Access activado en las 4 opciones, sin excepciones.
- **Cifrado en reposo**: SSE-S3 (`AES256`) por defecto en el bucket, para las tres
  capas.
- **Cifrado en tránsito**: bucket policy que deniega explícitamente cualquier
  request con `aws:SecureTransport: false`.
- **Sin secrets en logs**: los CLIs solo imprimen fechas, nombres de bucket, y
  conteos de filas — nunca un valor de credencial. El `ROLE_ARN` vive en GitHub Secrets;
  `BUCKET_NAME`/`AWS_REGION` en GitHub Variables (no son secretos, pero tampoco están
  hardcodeados en el YAML).
- **Validación de esquema en bronze**: `src/schema.py::validate_raw_event` rechaza
  eventos con campos requeridos ausentes o mal tipados antes de que lleguen a silver/gold.
- **PEP8 + type hints**: revisado con `ruff` (uso transitorio, no es dependencia del
  proyecto) sobre todo `src/`.

Todo esto se aprovisiona una sola vez, manualmente, con `infra/bootstrap.sh` — ver
[§10](#10-cómo-poner-en-marcha-contra-aws-real).

## 7. Testing

| Tipo | Ubicación | Contra AWS real | Cuándo corre |
|---|---|---|---|
| Unitarios | `tests/unit/` | No | Local y en Actions (cualquier `pytest`) |
| Integración | `tests/integration/` | Sí | Solo en Actions (auto-skip local) |

`tests/conftest.py` define el marcador `integration` con auto-skip: si `BUCKET_NAME` y
`AWS_REGION` no están en el entorno, cualquier test marcado `integration` se salta
automáticamente — así `pytest` sin argumentos siempre corre limpio en local, sin tocar
AWS.

Cada test de integración crea sus datos bajo un prefijo descartable
`_integration_tests/<uuid>/` y lo borra en un fixture `autouse` al terminar, para no
interferir con los datos reales del pipeline ni entre sí.

```bash
pytest tests/unit -v          # 20 tests, sin AWS
pytest tests/ -v              # + 5 tests de integración (SKIPPED en local)
```

## 8. CI/CD: GitHub Actions

Workflow: [`.github/workflows/pipeline.yml`](.github/workflows/pipeline.yml).
Disparadores: `workflow_dispatch` (manual, con inputs) y `schedule` (diario, 06:00 UTC).

### Inputs de `workflow_dispatch`

| Input | Requerido | Default | Descripción |
|---|---|---|---|
| `start_date` | Sí | — | Primera fecha del rango (`YYYY-MM-DD`) |
| `end_date` | No | `start_date` | Última fecha del rango |
| `start_stage` | No | `generate` | `generate` \| `bronze` \| `silver` \| `gold` |
| `seed` | No | `"42"` | Semilla del generador (solo si `start_stage=generate`) |

Contrato completo: [`specs/001-audience-metrics-poc/contracts/workflow-dispatch-inputs.md`](specs/001-audience-metrics-poc/contracts/workflow-dispatch-inputs.md).

### Jobs

```text
resolve → generate → bronze → silver → gold → integration_tests
```

- **`resolve`**: normaliza los inputs de `workflow_dispatch` y de `schedule` (que no
  tiene inputs — usa la fecha del día vía `date -u +%F`) a un único conjunto de valores
  que consumen los demás jobs.
- **`generate` / `bronze` / `silver` / `gold`**: cada uno corre solo si la etapa
  anterior tuvo éxito o fue salteada, y solo si `start_stage` lo permite (lógica de
  salteo vía `if:` con `needs.<job>.result`). `gold` siempre corre si `silver` terminó
  bien o fue salteado — nunca se saltea a sí mismo.
- **`integration_tests`**: corre `pytest tests -v` (unitarios + integración) contra el
  bucket real, con las mismas credenciales OIDC. Es el único lugar donde los tests
  marcados `integration` se ejecutan de verdad.

Cada job (excepto `resolve`) hace: checkout → setup Python 3.12 → `pip install -e .` →
`aws-actions/configure-aws-credentials` (OIDC) → el paso específico de su etapa.

## 9. Cómo ejecutar en local

Requisitos: Python 3.12.

```bash
pip install -e ".[dev]"
pytest tests/unit -v
python -m src.generator.events --seed 42 --date 2026-08-01 --out /tmp/eventos
```

Nada de esto toca AWS. Ver la guía completa (incluyendo los pasos contra AWS real):
[`specs/001-audience-metrics-poc/quickstart.md`](specs/001-audience-metrics-poc/quickstart.md).

## 10. Cómo poner en marcha contra AWS real

1. **Aprovisionar infraestructura una sola vez**, desde una máquina con credenciales de
   AWS propias (nunca desde el pipeline):

   ```bash
   AWS_REGION=us-east-1 \
   BUCKET_NAME=<nombre-unico-de-bucket> \
   GITHUB_ORG=<tu-org-o-usuario> \
   GITHUB_REPO=tv-audience-metrics-pipeline \
   ./infra/bootstrap.sh
   ```

   Esto crea (de forma re-ejecutable, verifica existencia antes de crear): el OIDC
   Identity Provider de GitHub, el IAM Role de mínimo privilegio, y el bucket S3
   privado y cifrado. Al final imprime `ROLE_ARN`, `BUCKET_NAME` y `AWS_REGION`.

2. **Configurar el repositorio de GitHub**:
   - Secret `ROLE_ARN` = el ARN que imprimió el script.
   - Variables `BUCKET_NAME` y `AWS_REGION` = los valores usados en el paso anterior.

3. **Disparar el pipeline**: Actions → "TV Audience Metrics Pipeline" → *Run workflow*,
   completando `start_date` (y opcionalmente `end_date`, `start_stage`, `seed`).

4. **Verificar el resultado**:

   ```bash
   aws s3 ls s3://<bucket>/gold/fecha=<fecha>/ --recursive
   ```

## 11. Estructura de directorios

```text
src/
├── schema.py              # Esquema compartido + validación
├── s3_io.py                 # download_partition / upload_partition (delete-then-write)
├── generator/events.py       # Generador reproducible + CLI
├── bronze/ingest.py           # Validación + normalización a Parquet + CLI
├── silver/clean.py             # Dedup + tipado + null-handling + CLI
└── gold/metrics.py              # Rating%/Share% + CLI

tests/
├── conftest.py              # Fixtures + auto-skip de tests `integration`
├── unit/                     # 20 tests, sin AWS
└── integration/               # 5 tests, solo contra S3 real en Actions

infra/
└── bootstrap.sh              # Aprovisionamiento manual, una sola vez (OIDC + IAM + bucket)

.github/workflows/
└── pipeline.yml               # resolve → generate → bronze → silver → gold → integration_tests

specs/001-audience-metrics-poc/
├── spec.md                   # Qué y por qué (historias de usuario, requisitos)
├── plan.md                    # Cómo (stack, arquitectura, constitution check)
├── research.md                  # Decisiones técnicas y alternativas descartadas
├── data-model.md                 # Entidades y esquemas
├── contracts/                     # Contrato de datos S3 + contrato de inputs del workflow
├── quickstart.md                   # Guía de validación end-to-end
└── tasks.md                         # Las 31 tareas de implementación (todas completadas)
```

## 12. Decisiones de diseño y alternativas descartadas

Resumen — el detalle completo con justificación está en
[`specs/001-audience-metrics-poc/research.md`](specs/001-audience-metrics-poc/research.md):

- **DuckDB nunca lee/escribe `s3://` directamente** (sin extensión `httpfs`); toda la
  I/O de S3 pasa por `boto3` en `src/s3_io.py`. *Alternativa descartada*: `httpfs`
  duplicaría la gestión de credenciales y dificultaría el delete-then-write atómico.
- **Bronze particiona solo por `fecha`**; silver y gold particionan por `fecha` +
  `canal`. *Razón*: bronze es una copia cruda 1:1 del lote diario; silver/gold sí se
  benefician de partition pruning por canal y de poder reprocesar un solo canal.
- **Sin Pydantic** para validación de esquema — un `dataclass` + funciones explícitas en
  `src/schema.py` es suficiente y no oculta el mecanismo. *Alternativa descartada*:
  Pydantic, por la constitución ("evitar librerías de conveniencia").
- **Sin Terraform/CDK** para el bootstrap de infraestructura — un script de AWS CLI
  (`infra/bootstrap.sh`) es suficiente para los ~4 recursos involucrados y no añade una
  herramienta nueva a un proyecto de aprendizaje. *Alternativa descartada*: Terraform,
  por el overhead de setup (state backend, providers) desproporcionado para la POC.
- **Franja horaria = bloques de 60 minutos**, truncando el timestamp a la hora —
  estándar de la industria de medición de audiencia de TV.
- **Comparación de idempotencia por hash de contenido lógico vía DuckDB**, nunca por
  bytes de Parquet ni por ETag/MD5 del objeto S3 — dos escrituras lógicamente idénticas
  pueden diferir en metadata binaria.
