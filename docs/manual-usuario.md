# Manual de Usuario — TV Audience Metrics Pipeline

Guía práctica para **disparar** el pipeline, **interpretar** sus resultados, y
**resolver** los problemas más comunes. No requiere leer el código — pensado para un
operador o analista que necesita correr el pipeline y consultar el reporte final.

Para el porqué de las decisiones técnicas, ver [`analisis-diseno.md`](analisis-diseno.md).
Para el detalle técnico exhaustivo, ver [`../DOCUMENTATION.md`](../DOCUMENTATION.md).

## Índice

1. [Qué hace este pipeline](#1-qué-hace-este-pipeline)
2. [Requisitos previos](#2-requisitos-previos)
3. [Cómo disparar una corrida manual](#3-cómo-disparar-una-corrida-manual)
4. [La corrida automática diaria](#4-la-corrida-automática-diaria)
5. [Cómo saber si terminó bien](#5-cómo-saber-si-terminó-bien)
6. [Cómo consultar los resultados](#6-cómo-consultar-los-resultados)
7. [Solución de problemas comunes](#7-solución-de-problemas-comunes)
8. [Preguntas frecuentes](#8-preguntas-frecuentes)

---

## 1. Qué hace este pipeline

Toma eventos de sintonía de TV (simulados) y calcula, para cada canal y cada hora del
día, dos métricas:

- **Rating%**: qué porcentaje del universo total estuvo viendo ese canal.
- **Share%**: de toda la audiencia que estuvo mirando TV en esa hora, qué porcentaje
  eligió ese canal.

El resultado queda guardado en el bucket S3 configurado, bajo `gold/fecha=.../canal=.../`,
en formato Parquet.

## 2. Requisitos previos

Para poder disparar corridas y ver resultados necesitás:

- Acceso al repositorio de GitHub (`gsalfate-code/tv-audience-metrics-pipeline`) con
  permiso para ejecutar workflows (Actions).
- Que el bucket S3 y las credenciales de AWS ya estén configurados como **secrets** del
  repositorio (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `BUCKET_NAME`) — esto lo
  hace una sola vez quien administra la infraestructura, no algo que se repite en cada
  corrida.
- Si querés consultar resultados desde tu máquina (no solo por correo): Python 3.12 y
  credenciales de AWS con permiso de lectura sobre el bucket, configuradas localmente
  (variables de entorno o AWS profile).

## 3. Cómo disparar una corrida manual

1. En GitHub, andá a la pestaña **Actions** del repositorio.
2. En la lista de la izquierda, elegí el workflow **"TV Audience Metrics Pipeline"**.
3. Hacé clic en **"Run workflow"** (arriba a la derecha) y completá:

| Campo | Obligatorio | Qué poner |
|---|---|---|
| `start_date` | Sí | Primera fecha a procesar, formato `YYYY-MM-DD` |
| `end_date` | No | Última fecha del rango; si lo dejás vacío, se usa `start_date` (un solo día) |
| `start_stage` | No | Desde qué etapa arrancar: `generate` (default, corre todo desde cero), `bronze`, `silver` o `gold` |
| `seed` | No | Semilla del generador de eventos simulados (default `42`); solo importa si `start_stage=generate` |
| `aws_region` | No | Región de AWS del bucket (default `us-east-1`) |

4. Confirmá con **"Run workflow"**.

**Cuándo usar `start_stage` distinto de `generate`**: si una corrida falló a mitad de
camino (por ejemplo en `silver`) y ya no querés repetir `generate`/`bronze` porque
tardan y no cambiaron, elegí `start_stage=silver` para retomar desde ahí sobre los
mismos datos ya subidos a S3.

## 4. La corrida automática diaria

El pipeline también corre solo, todos los días a las **06:00 UTC**, procesando la fecha
del día en curso con `seed=42` desde `generate`. No requiere ninguna acción manual — es
para tener un reporte diario sin depender de que alguien lo dispare.

## 5. Cómo saber si terminó bien

Al terminar cada corrida (manual o automática), llega un **correo** a
`gsalfate.code@gmail.com` con asunto:

```
[TV Metrics Pipeline] OK - 2026-08-01
```

o `FALLÓ` en vez de `OK` si alguna etapa falló. El cuerpo del correo lista el resultado
de cada job:

```
Rango: 2026-08-01 -> 2026-08-01
Etapa inicial: generate

generate:           success
bronze:             success
silver:             success
gold:               success
integration_tests:  success
```

Si preferís no esperar el correo, también podés ver el estado en vivo en la pestaña
**Actions** del repo, abriendo la corrida en curso.

## 6. Cómo consultar los resultados

### Opción A — Directo en S3

```bash
aws s3 ls s3://<bucket>/gold/fecha=2026-08-01/ --recursive
```

### Opción B — Con `scripts/query_gold.py` (recomendado)

Este script descarga la partición pedida y corre una consulta SQL sobre ella con
DuckDB, sin necesidad de tocar S3 a mano.

```bash
pip install -e ".[dev]"   # una sola vez

python scripts/query_gold.py --bucket <bucket> --start-date 2026-08-01
```

Consultas predefinidas (`--query <nombre>`):

| Nombre | Qué muestra |
|---|---|
| `rating_share` (default) | Rating%/Share% por fecha, franja horaria y canal |
| `resumen_canal` | Promedio de Rating%/Share% y audiencia acumulada por canal y día |
| `top_franja` | El canal líder (mayor Rating%) en cada franja horaria |
| `cobertura` | Franjas horarias sin ninguna sintonización (deben existir con audiencia 0, no faltar) |
| `share_check` | Verifica el invariante: suma de Share% por franja debe dar ~100 |

Ejemplos:

```bash
# Resumen por canal para una semana
python scripts/query_gold.py --bucket <bucket> --start-date 2026-08-01 \
  --end-date 2026-08-07 --query resumen_canal

# Verificar que el Share% suma 100 en cada franja
python scripts/query_gold.py --bucket <bucket> --start-date 2026-08-01 --query share_check

# SQL propio
python scripts/query_gold.py --bucket <bucket> --start-date 2026-08-01 \
  --sql "SELECT canal, MAX(rating_pct) FROM gold GROUP BY canal"

# Ver todas las consultas disponibles
python scripts/query_gold.py --list-queries
```

Si no pasás `--bucket` o `--region`, el script los toma de `$BUCKET_NAME` y
`$AWS_REGION`/`$AWS_DEFAULT_REGION` si están definidos en el entorno.

## 7. Solución de problemas comunes

### El correo de notificación no llega / falla con "Invalid login: 535-5.7.8"

Es un problema de credenciales de Gmail, no del pipeline en sí. Pasos:

1. Confirmá que la cuenta de Gmail (`GMAIL_USERNAME`) tenga **verificación en 2 pasos**
   activada — sin eso no existen las "Contraseñas de aplicación".
2. Generá una **App Password nueva** en
   `https://myaccount.google.com/apppasswords` (cualquier nombre sirve, es solo una
   etiqueta).
3. Actualizala en GitHub: **Settings → Secrets and variables → Actions → pestaña
   Secrets** (no "Variables" — son dos pestañas distintas y solo `secrets.*` tiene
   efecto acá) → `GMAIL_APP_PASSWORD`.
4. Si sigue fallando con credenciales confirmadas como correctas, revisá si Google
   bloqueó el inicio de sesión por venir de una IP de datacenter (los runners de GitHub
   Actions): buscá un correo de Google *"Se impidió el acceso a tu cuenta"* y confirmá
   "Sí, fui yo", o visitá `https://accounts.google.com/DisplayUnlockCaptcha` con esa
   cuenta.
5. **Nunca compartas la contraseña de aplicación por chat, correo o cualquier canal
   registrado** — si se expone accidentalmente, revocala y generá una nueva
   inmediatamente.

### El job falla con un error de credenciales de AWS

El pipeline usa credenciales estáticas guardadas en los secrets `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` del repositorio (ver nota en
[`analisis-diseno.md §9`](analisis-diseno.md#9-seguridad--estado-real-vs-diseño-original)).
Si expiraron o fueron rotadas, hay que generar unas nuevas en IAM y actualizar esos dos
secrets.

### El job falla porque no encuentra el bucket

El nombre del bucket se lee de `secrets.BUCKET_NAME` (no de "Variables"). Confirmá que
ese secret exista y tenga el nombre exacto del bucket.

### Corrí el pipeline dos veces sobre la misma fecha, ¿se duplican los datos?

No — el pipeline es idempotente por diseño: cada partición se reemplaza por completo
antes de escribir (`delete-then-write`), nunca se anexa. Re-ejecutar sobre el mismo
rango produce el mismo resultado. Ver
[`analisis-diseno.md §8`](analisis-diseno.md#8-decisiones-de-diseño-clave).

### Quiero reprocesar solo una fecha que falló en `silver`

Disparás el workflow con `start_stage=silver` y el mismo `start_date`/`end_date` de la
corrida original — no hace falta repetir `generate` ni `bronze`.

## 8. Preguntas frecuentes

**¿Necesito saber Python o SQL para usar el pipeline?**
No para dispararlo (es un botón en GitHub Actions). Para consultar resultados más allá
de lo que trae el correo, sí conviene SQL básico si usás `scripts/query_gold.py --sql`.

**¿Puedo correr algo localmente sin tocar AWS?**
Sí — `python -m src.generator.events --seed 42 --date 2026-08-01 --out /tmp/eventos`
genera eventos simulados sin bucket. Los tests unitarios (`pytest tests/unit`) tampoco
tocan AWS.

**¿Dónde veo la definición completa de Rating% y Share%?**
En [`analisis-diseno.md §7`](analisis-diseno.md#7-modelo-de-datos) y, con más detalle
técnico, en [`../DOCUMENTATION.md §3`](../DOCUMENTATION.md#3-modelo-de-datos).
