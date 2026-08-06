<!--
Sync Impact Report
- Version change: (none — template) → 1.0.0
- Rationale: Initial ratification. First concrete adoption of all governing
  principles for this project (MAJOR per semver policy for a 0→1 baseline).
- Modified principles: N/A (initial creation, no prior filled version existed)
- Added sections:
  - I. Idempotencia y Determinismo
  - II. Simplicidad y Purismo (Aprendizaje Autodidacta)
  - III. Arquitectura Medallion y Datos
  - IV. Testing Riguroso
  - V. Seguridad y Mínimo Privilegio
  - VI. Control de Versiones y Orquestación
  - VII. Ejecución Exclusiva en GitHub Actions
  - Governance
- Removed sections: generic [SECTION_2_NAME] / [SECTION_3_NAME] template
  slots — not needed, all guidance from user input fit within the seven
  core principles above.
- Templates checked for consistency:
  - .specify/templates/plan-template.md — references a generic
    "Constitution Check" gate with no hardcoded principle names; no edit
    required. ✅
  - .specify/templates/spec-template.md — no constitution references. ✅
  - .specify/templates/tasks-template.md — no constitution references. ✅
  - .specify/templates/checklist-template.md — no constitution references. ✅
- Follow-up TODOs: none — all placeholders resolved from user input.
-->

# TV Audience Metrics Pipeline Constitution

## Core Principles

### I. Idempotencia y Determinismo
Reprocesar el mismo batch de datos DEBE producir exactamente el mismo
resultado: sin duplicados y sin efectos acumulativos.
- Mecánica obligatoria de idempotencia: antes de escribir una partición
  (fecha/canal), se borran los objetos existentes en ese prefijo S3 y
  luego se escriben los nuevos (patrón delete-then-write).
- PROHIBIDO usar funciones no deterministas (`random()`, `now()`,
  timestamps de wall-clock) dentro de la lógica de transformación de
  negocio.
- Cualquier timestamp de proceso se registra como metadata separada
  (por ejemplo, una columna de auditoría fuera de las claves de
  negocio), nunca como parte de una clave de partición o de agregación.

**Rationale**: sin idempotencia estricta, un reintento de CI o un
backfill manual duplica filas o produce agregados distintos en cada
corrida, lo que destruye la confianza en las métricas de audiencia.

### II. Simplicidad y Purismo (Aprendizaje Autodidacta)
Este es un proyecto de aprendizaje autodidacta: el código explícito y
legible tiene prioridad sobre abstracciones "elegantes".
- PROHIBIDO usar factories, capas de indirección innecesarias, o config
  management sobre-diseñado.
- Cada función DEBE poder leerse de arriba a abajo sin saltar entre más
  de un archivo adicional.
- Stack limitado a lo estrictamente necesario: `boto3` (o `s3fs`) para
  S3, `duckdb` para todo el procesamiento, `pytest` para tests.
  PROHIBIDO introducir ORMs o frameworks de orquestación tipo
  Dagster/Prefect/Airflow.
- Toda decisión de diseño DEBE poder explicarse en una frase simple; si
  no se puede, hay que simplificarla.

**Rationale**: el objetivo del proyecto es entender el mecanismo
subyacente de un pipeline de datos, no evaluar frameworks; cualquier
capa de conveniencia que oculte ese mecanismo va en contra del
propósito educativo.

### III. Arquitectura Medallion y Datos
- Arquitectura de tres capas — bronze (crudo) → silver
  (limpio/conformado) → gold (agregados de negocio) — cada una en su
  propio prefijo de S3.
- Toda manipulación de datos usa la API relacional de DuckDB en Python
  (`duckdb.sql` / conexión activa durante la ejecución). PROHIBIDO usar
  pandas para transformaciones pesadas.
- Particionamiento Hive (`fecha=.../canal=...`) obligatorio tanto al
  escribir como al leer, para permitir partition pruning.
- Cada job de GitHub Actions descarga los objetos S3 relevantes al
  disco efímero del runner, ejecuta las transformaciones localmente, y
  sube los resultados de vuelta a S3. No se asume persistencia de
  ningún archivo `.duckdb` entre ejecuciones.

**Rationale**: la separación medallion hace explícito el contrato de
calidad en cada etapa, y DuckDB sobre disco efímero evita depender de
infraestructura con estado que un proyecto de aprendizaje no necesita
mantener.

### IV. Testing Riguroso
- Cobertura obligatoria con `pytest`.
- Los tests unitarios (lógica de transformación, cálculo de métricas)
  corren sin tocar AWS real.
- Los tests de integración contra S3 real corren únicamente dentro del
  workflow de GitHub Actions.
- Test de idempotencia obligatorio: correr el pipeline dos veces sobre
  el mismo input y comparar el contenido lógico de los datos (leído vía
  DuckDB, filas ordenadas y hasheadas), nunca los bytes crudos del
  archivo Parquet.

**Rationale**: comparar bytes crudos de Parquet genera falsos negativos
(metadata, orden de row groups, compresión pueden variar sin cambiar el
contenido lógico); el hash sobre filas ordenadas es la única
comparación fiel al principio de idempotencia.

### V. Seguridad y Mínimo Privilegio
- Sin credenciales hardcodeadas: acceso a S3 vía variables de entorno o
  AWS profiles en local, y OIDC role assumption desde GitHub Actions —
  nunca access keys de larga duración.
- El IAM Role usado por OIDC tiene permisos acotados exactamente a
  `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, restringidos por
  ARN al bucket/prefijo del proyecto. PROHIBIDO `"s3:*"` o
  `"Resource": "*"`.
- Buckets S3 privados por defecto: Block Public Access activado a nivel
  de bucket, sin excepciones.
- Cifrado en reposo (SSE-S3) habilitado por defecto para las tres capas
  (bronze/silver/gold).
- Cifrado en tránsito: las políticas del bucket fuerzan HTTPS,
  denegando explícitamente conexiones no-TLS.
- Los workflows de GitHub Actions no imprimen valores de credenciales,
  ARNs sensibles, ni contenido de Secrets en la salida de los steps.
- La capa bronze valida el esquema de los eventos de entrada (tipos
  correctos, campos requeridos presentes) antes de aceptarlos, para no
  propagar datos corruptos silenciosamente hacia silver/gold.
- Código Python sigue PEP8; type hints obligatorios en funciones
  públicas.

**Rationale**: en un proyecto que toca AWS real, el mínimo privilegio y
el cifrado por defecto no son negociables aunque el propósito sea
educativo — son también el objeto de aprendizaje.

### VI. Control de Versiones y Orquestación
- Repositorio en GitHub, con Pull Requests para todo cambio; rama
  principal protegida, sin push directo.
- GitHub Actions ejecuta el pipeline (`workflow_dispatch` manual y/o
  `schedule` cron), separando bronze → silver → gold en steps claros,
  con posibilidad de re-ejecutar solo desde una etapa específica.
- Autenticación GitHub Actions → AWS vía OIDC (IAM Identity Provider +
  Role con trust policy restringida al repo/branch).
- Secrets de AWS gestionados vía GitHub Secrets, nunca en código ni en
  el workflow YAML.

**Rationale**: exigir PRs y steps separables hace que cada etapa del
pipeline sea auditable y re-ejecutable de forma aislada, lo cual es
también la mecánica que se quiere aprender.

### VII. Ejecución Exclusiva en GitHub Actions
El pipeline corre exclusivamente en GitHub Actions contra AWS real; el
entorno local (VS Code) se usa solo para desarrollo, tests unitarios y
revisión de código.

**Rationale**: mantener un único lugar de ejecución contra datos reales
elimina la deriva entre "funciona en mi máquina" y el comportamiento en
producción, y evita el uso accidental de credenciales locales contra
AWS real.

## Governance

Esta constitución prevalece sobre cualquier otra práctica, plantilla o
convención informal del proyecto. Ante un conflicto entre esta
constitución y un artefacto de Spec Kit (plan, tasks, checklist), la
constitución tiene prioridad y el artefacto en conflicto debe
corregirse.

**Procedimiento de enmienda**: cualquier cambio a esta constitución se
propone vía Pull Request contra `main`, describiendo explícitamente el
principio afectado y la motivación del cambio. El PR debe actualizar el
Sync Impact Report al inicio de este archivo y, si corresponde, señalar
qué plantillas dependientes (`plan-template.md`, `spec-template.md`,
`tasks-template.md`, `checklist-template.md`) requieren ajuste.

**Política de versionado semántico**:
- MAJOR: eliminación o redefinición incompatible de un principio
  existente.
- MINOR: adición de un nuevo principio o expansión material de guía
  existente.
- PATCH: aclaraciones, correcciones de redacción, cambios no
  semánticos.

**Revisión de cumplimiento**: todo PR que toque código de pipeline
(bronze/silver/gold), workflows de GitHub Actions, o definiciones de
infraestructura (IAM, políticas de bucket) DEBE verificar
explícitamente en su descripción que cumple los principios I–VII de
esta constitución. Cualquier excepción debe justificarse por escrito en
el propio PR; sin justificación, el PR no es mergeable.

**Version**: 1.0.0 | **Ratified**: 2026-08-06 | **Last Amended**: 2026-08-06
