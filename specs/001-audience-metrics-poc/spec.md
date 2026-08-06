# Feature Specification: POC Pipeline de Métricas de Audiencia de TV

**Feature Branch**: `001-audience-metrics-poc`

**Created**: 2026-08-06

**Status**: Draft

**Input**: User description: "Construir una POC de pipeline de datos para un canal de TV que calcula métricas de audiencia (Rating% y Share%) a partir de eventos de sintonía.

- Fuente de datos: eventos simulados de \"sintonía por minuto\" (timestamp, canal, id_hogar/panelista, universo_total), generados de forma reproducible (semilla fija).
- Ingesta (bronze): los eventos se suben a S3 y se normalizan a formato Parquet, sin lógica de negocio, particionados por fecha.
- Transformación (silver): los datos se limpian (deduplicación por clave natural, tipado, manejo de nulos) y se escriben particionados de forma consistente.
- Agregación (gold): se calcula Rating% (audiencia del canal / universo total) y Share% (audiencia del canal / audiencia total sintonizando) por canal y franja horaria, y se emite como reporte final en Parquet.
- El pipeline completo debe ser re-ejecutable sobre el mismo rango de fechas sin generar duplicados ni resultados distintos (idempotente y determinista).
- Debe poder correr en GitHub Actions contra un bucket S3 real en AWS, usando las credenciales inyectadas vía OIDC (nunca access keys locales)."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Obtener Rating% y Share% por canal y franja horaria (Priority: P1)

Como analista de audiencia, quiero obtener el Rating% y el Share% de cada canal por franja horaria a partir de eventos de sintonía simulados, para poder validar el cálculo de métricas de audiencia sin depender de datos reales de panel.

**Why this priority**: es el entregable de negocio de la POC — sin el reporte final de Rating%/Share%, el resto del pipeline no tiene valor demostrable por sí solo.

**Independent Test**: se puede probar generando un lote de eventos simulados con semilla fija para un rango de fechas acotado, ejecutando el pipeline completo, y verificando que el reporte final contiene un Rating% y un Share% por canal y franja horaria cuyos valores son matemáticamente consistentes con los eventos de entrada (verificables a mano para un caso pequeño).

**Acceptance Scenarios**:

1. **Given** un conjunto de eventos de sintonía simulados para una fecha y un conjunto de canales conocidos, **When** se ejecuta el pipeline completo, **Then** el reporte final contiene, para cada combinación canal + franja horaria, un Rating% igual a la audiencia del canal dividida por el universo total, y un Share% igual a la audiencia del canal dividida por la audiencia total sintonizando en esa franja.
2. **Given** un canal sin ningún evento de sintonía en una franja horaria determinada, **When** se ejecuta el pipeline, **Then** ese canal aparece en el reporte con Rating% y Share% en cero para esa franja (no se omite silenciosamente).
3. **Given** el reporte final generado, **When** se suman los valores de Share% de todos los canales dentro de una misma franja horaria, **Then** la suma es igual al 100% (o queda indeterminada solo cuando no hubo ninguna sintonía en esa franja, ver Edge Cases).

---

### User Story 2 - Re-ejecutar el pipeline sin duplicar ni alterar resultados (Priority: P2)

Como operador del pipeline, quiero poder re-ejecutar el procesamiento sobre el mismo rango de fechas (por ejemplo, tras un reintento fallido o un backfill manual) y obtener exactamente el mismo resultado, para poder confiar en que los reintentos son seguros y no corrompen las métricas publicadas.

**Why this priority**: sin esta garantía, cualquier reintento o backfill introduce riesgo de duplicados o de métricas inconsistentes, lo cual invalida la confiabilidad del reporte de la Historia 1.

**Independent Test**: se puede probar ejecutando el pipeline completo dos veces seguidas sobre el mismo rango de fechas y el mismo dataset de entrada, y comparando el contenido lógico de cada capa (bronze, silver, gold) entre ambas corridas — deben ser idénticos, sin filas duplicadas ni valores distintos.

**Acceptance Scenarios**:

1. **Given** un rango de fechas ya procesado por el pipeline, **When** se vuelve a ejecutar el pipeline completo sobre el mismo rango sin cambios en los datos de entrada, **Then** el número de filas y el contenido de cada capa (bronze, silver, gold) para ese rango es idéntico al de la primera ejecución.
2. **Given** una ejecución previa exitosa para una fecha y canal específicos, **When** se re-ejecuta el pipeline solo para esa fecha y canal, **Then** únicamente los datos de esa partición (fecha/canal) se reemplazan, sin afectar otras particiones ya existentes.

---

### User Story 3 - Ejecutar el pipeline de forma automatizada y segura en la nube (Priority: P3)

Como operador del pipeline, quiero poder disparar la ejecución completa (generación de eventos simulados, ingesta, transformación y agregación) desde un entorno de automatización en la nube contra un bucket S3 real, sin usar credenciales de AWS de larga duración, para que la POC sea operable de forma repetible y segura más allá de mi máquina local.

**Why this priority**: valida que la POC no es solo un script local, sino un pipeline productivo mínimo, ejecutable de forma auditable y sin manejo manual de secretos — requisito explícito del proyecto.

**Independent Test**: se puede probar disparando manualmente la automatización (sin cambios de código) y verificando que el reporte final aparece en el bucket S3 real al finalizar, sin que ninguna credencial de larga duración haya sido usada ni expuesta en los logs de ejecución.

**Acceptance Scenarios**:

1. **Given** el pipeline configurado en el entorno de automatización, **When** se dispara manualmente una ejecución, **Then** el proceso obtiene credenciales de AWS de forma temporal (sin claves de acceso almacenadas) y completa las tres etapas (bronze, silver, gold) escribiendo los resultados en el bucket S3 real.
2. **Given** una ejecución del pipeline en el entorno de automatización, **When** se revisan los logs de la ejecución, **Then** no aparece ningún valor de credencial ni secreto en texto plano.
3. **Given** una falla en la etapa de transformación (silver), **When** el operador decide reintentar, **Then** puede volver a ejecutar únicamente desde esa etapa en adelante sin repetir la generación de eventos ni la ingesta ya completada.

---

### Edge Cases

- ¿Qué ocurre si dos eventos de sintonía llegan con la misma clave natural (mismo hogar/panelista, canal y minuto) pero valores levemente distintos (por ejemplo, universo_total diferente)? El pipeline debe resolverlo de forma determinista (misma resolución en cada corrida), sin duplicar el hogar/panelista en el conteo de audiencia.
- ¿Qué ocurre en una franja horaria en la que ningún hogar/panelista estuvo sintonizando ningún canal? El Share% de todos los canales en esa franja queda indefinido (audiencia total sintonizando = 0); el reporte debe representar este caso explícitamente en vez de mostrar un error o un valor engañoso como 0/0.
- ¿Qué ocurre si un evento llega con un campo requerido nulo o de tipo incorrecto (por ejemplo, canal vacío o timestamp no parseable)? El evento debe rechazarse antes de silver, sin detener el procesamiento del resto del lote, y debe quedar registrado que fue descartado.
- ¿Qué ocurre si `universo_total` cambia de un evento a otro dentro del mismo canal y franja horaria (inconsistencia en la fuente)? El pipeline debe aplicar una regla de resolución determinista y documentada (por ejemplo, el valor más reciente por timestamp del evento, nunca por hora de procesamiento) para no producir un Rating% ambiguo.
- ¿Qué ocurre si se solicita reprocesar una fecha para la cual nunca se generaron eventos? El pipeline debe completar sin error y producir capas vacías para esa fecha, no fallar.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: El sistema DEBE generar eventos simulados de sintonía por minuto (timestamp, canal, identificador de hogar/panelista, universo_total) de forma reproducible: la misma semilla y los mismos parámetros de entrada DEBEN producir siempre el mismo conjunto de eventos.
- **FR-002**: El sistema DEBE ingerir los eventos generados y almacenarlos en su forma cruda (capa bronze), normalizados a un formato columnar, sin aplicar ninguna regla de negocio (sin deduplicar, sin calcular métricas), particionados por fecha.
- **FR-003**: El sistema DEBE transformar los datos de la capa bronze a una capa limpia (silver), aplicando: deduplicación por clave natural (hogar/panelista + canal + minuto), tipado correcto de cada campo, y una regla explícita de manejo de valores nulos o inválidos.
- **FR-004**: El sistema DEBE rechazar en la capa de ingesta (bronze) los eventos que no cumplan el esquema esperado (tipos correctos, campos requeridos presentes), evitando que datos corruptos avancen silenciosamente hacia silver o gold.
- **FR-005**: El sistema DEBE calcular, a partir de la capa silver, el Rating% de cada canal por franja horaria como: audiencia del canal en esa franja / universo total.
- **FR-006**: El sistema DEBE calcular, a partir de la capa silver, el Share% de cada canal por franja horaria como: audiencia del canal en esa franja / audiencia total sintonizando (suma de audiencia de todos los canales) en esa franja.
- **FR-007**: El sistema DEBE emitir el resultado de Rating%/Share% como un reporte final (capa gold) en formato columnar, con una fila por combinación de fecha, canal y franja horaria.
- **FR-008**: El sistema DEBE permitir re-ejecutar el pipeline completo (o una etapa específica) sobre un rango de fechas ya procesado, y el resultado lógico de cada capa DEBE ser idéntico al de la ejecución original, sin filas duplicadas.
- **FR-009**: El sistema DEBE reemplazar por completo el contenido de una partición (fecha/canal) antes de escribir sus nuevos datos, en vez de anexar filas a una partición existente.
- **FR-010**: El sistema NO DEBE usar valores no deterministas (aleatoriedad sin semilla fija, la hora actual del sistema, etc.) dentro del cálculo de eventos ni de métricas; cualquier marca de tiempo de proceso (auditoría) DEBE almacenarse por separado de los datos de negocio.
- **FR-011**: El sistema DEBE poder ejecutarse de punta a punta (generación de eventos, bronze, silver, gold) de forma manual desde un entorno de automatización en la nube, contra un almacenamiento real, sin que un operador humano provea claves de acceso de larga duración.
- **FR-012**: El sistema DEBE permitir reanudar o re-disparar la ejecución desde una etapa intermedia específica (por ejemplo, solo desde silver en adelante) sin tener que repetir las etapas ya completadas exitosamente.
- **FR-013**: El sistema NO DEBE exponer credenciales ni secretos en la salida/logs de una ejecución automatizada.
- **FR-014**: El sistema DEBE representar explícitamente, en el reporte final, las combinaciones canal + franja horaria sin audiencia (Rating% y Share% en cero), en vez de omitirlas.
- **FR-015**: El sistema DEBE aplicar una regla de resolución determinista y documentada cuando existan valores en conflicto para una misma clave natural (por ejemplo, universo_total distinto entre eventos duplicados).

### Key Entities

- **Evento de sintonía (crudo)**: registro fuente que representa que un hogar/panelista estuvo sintonizando un canal durante un minuto determinado. Atributos: timestamp (minuto), canal, identificador de hogar/panelista, universo_total vigente en ese momento.
- **Evento de sintonía (limpio)**: versión deduplicada y tipada del evento crudo, una fila única por combinación hogar/panelista + canal + minuto, sin nulos en campos requeridos.
- **Franja horaria**: agrupación temporal (por ejemplo, cada hora del día) usada como unidad de agregación para el reporte final; agrupa múltiples eventos de sintonía por minuto.
- **Métrica de audiencia (reporte final)**: fila agregada por fecha + canal + franja horaria, con los valores de Rating% y Share% calculados y el conteo de audiencia y universo total utilizados en el cálculo.
- **Universo total**: tamaño de la población de referencia contra la que se calcula el Rating%; viaja como parte del evento de sintonía.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Para un lote de eventos simulados de un día completo, el pipeline produce el reporte final de Rating%/Share% en menos de 10 minutos de ejecución de punta a punta.
- **SC-002**: Al ejecutar el pipeline dos veces sobre el mismo rango de fechas y los mismos datos de entrada, el contenido lógico de cada capa (bronze, silver, gold) es 100% idéntico entre ambas ejecuciones (cero filas duplicadas, cero valores distintos).
- **SC-003**: El 100% de los eventos de entrada con esquema inválido son rechazados antes de llegar a la capa silver, sin detener el procesamiento del resto del lote.
- **SC-004**: Para cualquier franja horaria con al menos una sintonización, la suma de Share% de todos los canales en esa franja es 100% (dentro de un margen de redondeo despreciable).
- **SC-005**: Una persona sin conocimiento previo del pipeline puede disparar una ejecución completa desde el entorno de automatización y encontrar el reporte final en el almacenamiento de resultados, siguiendo únicamente la documentación del proyecto, sin asistencia adicional.
- **SC-006**: Ninguna ejecución registrada en el entorno de automatización expone, en sus logs, un valor de credencial o secreto en texto plano.

## Assumptions

- El generador de eventos simulados es parte del alcance de la POC (no se asume una fuente de datos externa real); "semilla fija" significa que los mismos parámetros de entrada (semilla, rango de fechas, canales, cantidad de hogares/panelistas) producen siempre el mismo dataset.
- "Franja horaria" se interpreta como bloques de una hora (00:00–00:59, 01:00–01:59, etc.), el estándar habitual en reportes de audiencia de TV, salvo que se indique lo contrario más adelante.
- El alcance de la POC es una demostración funcional del pipeline (volumen de datos moderado, pensado para validar el mecanismo end-to-end), no una prueba de rendimiento a escala de producción real.
- "id_hogar/panelista" se trata como un identificador único de la unidad de medición de audiencia (hogar o panelista, según el modelo de medición simulado), sin distinguir entre ambos tipos en el cálculo de métricas.
- El reporte final (capa gold) es consumido como archivo de datos (no se incluye una capa de visualización/dashboard en el alcance de esta POC).
- No se requiere autenticación ni control de acceso por usuario final para consultar el reporte final más allá de los permisos de acceso al almacenamiento ya definidos a nivel de infraestructura.
