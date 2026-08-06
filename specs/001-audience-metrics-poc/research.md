# Research: POC Pipeline de Métricas de Audiencia de TV

No quedaron marcadores `[NEEDS CLARIFICATION]` sin resolver en el spec ni en el Technical
Context del plan (ver Assumptions en spec.md). Este documento registra las decisiones de
diseño/técnicas derivadas de los inputs del usuario y de la constitución, con su
justificación y alternativas descartadas, para que queden trazables antes de Phase 1.

## 1. Acceso a S3: descarga local + DuckDB local, no lectura remota vía httpfs

**Decision**: Toda lectura/escritura de S3 pasa exclusivamente por `boto3` en
`src/s3_io.py` (descargar objetos de una partición a un directorio temporal local, y
subir un directorio local a un prefijo S3). DuckDB solo lee/escribe archivos ya presentes
en disco local; nunca se le dan credenciales de AWS ni se usa su extensión `httpfs` para
leer `s3://` directamente.

**Rationale**: es un requisito explícito del usuario y de la constitución ("cada job
descarga los objetos S3 relevantes al disco efímero del runner, ejecuta las
transformaciones localmente, y sube los resultados"). Además concentra toda la lógica de
credenciales/red en un único módulo pequeño (`s3_io.py`), lo que simplifica los tests
unitarios (no necesitan mockear DuckDB ni AWS) y hace el patrón delete-then-write
explícito y fácil de verificar en un solo lugar.

**Alternatives considered**: usar la extensión `httpfs` de DuckDB para leer/escribir
`s3://` directamente — descartado porque duplicaría la gestión de credenciales en dos
lugares (boto3 para operaciones de borrado/listado, httpfs para lectura/escritura) y
dificultaría implementar delete-then-write de forma atómica y testeable.

## 2. Particionamiento: bronze solo por fecha, silver y gold por fecha + canal

**Decision**: `bronze/fecha=YYYY-MM-DD/` (sin partición por canal). `silver/fecha=YYYY-MM-DD/canal=<canal>/`
y `gold/fecha=YYYY-MM-DD/canal=<canal>/` sí particionan por ambos.

**Rationale**: el spec (FR-002) pide bronze particionado solo por fecha — es una carga
cruda 1:1 del lote de ingesta diario, sin lógica de negocio, así que partir por canal ahí
no aporta nada y añadiría un paso de interpretación de datos en una capa que debe
permanecer "sin lógica de negocio". Silver y gold sí se benefician de partition pruning
por canal porque las consultas de negocio (deduplicación, agregación de Rating%/Share%)
naturalmente filtran por canal, y el borrado idempotente por partición (FR-009) necesita
granularidad fecha+canal para poder reprocesar un solo canal sin tocar los demás (Historia
de Usuario 2, escenario 2 del spec).

**Alternatives considered**: particionar bronze también por canal — descartado por no
aportar valor en una capa cruda y por complicar la trazabilidad "un archivo de ingesta =
un prefijo de fecha".

## 3. Regla de resolución de conflictos en deduplicación (silver)

**Decision**: la clave natural es (`id_hogar_panelista`, `canal`, `timestamp_minuto`). Si
llegan dos eventos crudos con la misma clave natural pero valores distintos (p. ej.
`universo_total` distinto), silver se queda con el evento cuyo `timestamp` de evento
(dato de negocio, no de proceso) sea más reciente; en empate exacto de timestamp, se
resuelve con un criterio de orden estable y determinista sobre los demás campos (p. ej.
orden lexicográfico de la fila completa), nunca por orden de llegada/procesamiento.

**Rationale**: cumple FR-015 y el principio de determinismo (I) — la regla no depende de
en qué orden boto3 listó los objetos ni de cuándo corrió el job, solo del contenido de los
datos.

**Alternatives considered**: quedarse con "el primer evento visto" — descartado porque el
orden de lectura de archivos Parquet/objetos S3 no está garantizado y rompería el
determinismo.

## 4. Validación de esquema en bronze

**Decision**: `src/schema.py` define, con `dataclasses` + type hints estándar de Python
(sin Pydantic ni otra librería de validación), el esquema esperado del evento crudo
(nombre de columna, tipo, si es requerido). `src/bronze/ingest.py` valida cada evento
contra ese esquema antes de escribirlo; los eventos inválidos se excluyen del Parquet de
bronze y se cuentan/registran (no se detiene el job, ver Edge Cases del spec).

**Rationale**: cumple FR-004 y el principio de simplicidad (II) — no se introduce una
librería de validación adicional cuando un `dataclass` + funciones de chequeo explícitas
son suficientes y quedan legibles de arriba a abajo.

**Alternatives considered**: Pydantic — descartado por la constitución ("evitar librerías
de conveniencia que oculten el mecanismo por debajo").

## 5. Comparación de idempotencia en tests

**Decision**: el test de idempotencia (`tests/integration/test_pipeline_idempotency.py`)
corre el pipeline dos veces sobre el mismo rango de fechas y, para cada capa, lee los
datos resultantes vía DuckDB, ordena las filas por sus columnas (incluida la partición) y
calcula un hash agregado determinista sobre el contenido (por ejemplo, `md5(string_agg(...
ORDER BY ...))` en SQL de DuckDB, excluyendo cualquier columna de metadata de proceso como
timestamp de auditoría). Se comparan los hashes de ambas corridas.

**Rationale**: cumple el gate de testing de la constitución explícitamente: "comparar el
contenido lógico de los datos ..., nunca los bytes crudos del archivo Parquet" — dos
escrituras de Parquet lógicamente idénticas pueden diferir en bytes (metadata, orden de
row groups, compresión).

**Alternatives considered**: comparar checksums de los objetos S3 (ETag/MD5 del archivo)
— descartado explícitamente por la constitución por la razón anterior.

## 6. Franja horaria

**Decision**: bloques horarios fijos de 60 minutos (`00:00–00:59`, `01:00–01:59`, ...),
derivados truncando el `timestamp` del evento a la hora.

**Rationale**: estándar de la industria de medición de audiencia de TV para este tipo de
reporte; ya documentado como supuesto en `spec.md` (sección Assumptions).

**Alternatives considered**: bloques de 15/30 minutos — descartado por no estar pedido y
por aumentar el volumen del reporte gold sin necesidad para una POC.

## 7. Aprovisionamiento de infraestructura (OIDC + bucket)

**Decision**: un script único `infra/bootstrap.sh` basado en AWS CLI (no Terraform/CDK/
CloudFormation) que crea, de forma re-ejecutable de forma segura (verifica existencia
antes de crear), el IAM Identity Provider de GitHub, el IAM Role con trust policy
restringida al repo `tv-audience-metrics-pipeline`, y el bucket S3 con Block Public
Access + SSE-S3 + bucket policy que deniega tráfico no-TLS. Se ejecuta manualmente una
sola vez desde el entorno local, fuera del pipeline de Actions.

**Rationale**: la constitución limita el stack a "lo estrictamente necesario" y este es un
paso de aprovisionamiento único, no parte del pipeline recurrente; introducir Terraform/
CDK para una POC de aprendizaje añadiría una herramienta más a aprender sin necesidad,
cuando la CLI de AWS ya es explícita y suficiente para los ~4 recursos involucrados.

**Alternatives considered**: Terraform — descartado por overhead de setup (state backend,
providers) desproporcionado para el alcance de la POC; consola de AWS manual sin script —
descartado porque no sería reproducible ni documentable como parte del repo.

## 8. Autenticación OIDC en GitHub Actions

**Decision**: usar la acción oficial `aws-actions/configure-aws-credentials` con
`permissions: id-token: write` en el workflow, apuntando al Role creado por
`infra/bootstrap.sh`. Ninguna clave de acceso se guarda en GitHub Secrets; solo el ARN del
Role y el nombre del bucket (no sensibles) se guardan como variables/secrets de
repositorio para no hardcodearlos en el YAML.

**Rationale**: es el mecanismo estándar y soportado oficialmente por GitHub y AWS para
autenticación OIDC de corta duración, cumpliendo FR-011/FR-013 y el principio V de la
constitución sin código de autenticación custom.

**Alternatives considered**: access keys de larga duración en GitHub Secrets — descartado
explícitamente por el spec y la constitución.
