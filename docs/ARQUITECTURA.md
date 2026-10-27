# Arquitectura Técnica - SismoWatch

## 1. Visión General

SismoWatch es una plataforma de monitoreo sísmico que implementa un **lakehouse** con **arquitectura medallón**. Los datos se obtienen desde la API pública de USGS, se procesan mediante un pipeline orquestado con Apache Airflow y se almacenan, transforman y curan en **PostgreSQL 16** mediante los esquemas `bronze`, `silver` y `gold`. **MinIO (S3-compatible)** se utiliza de forma complementaria como almacenamiento objeto para artefactos. La plataforma se expone vía **FastAPI** y se visualiza en un frontend web con asistente IA local.

## 2. Diseño por Capas (Arquitectura Medallón)

| Capa | Esquema/Almacenamiento | Formato | Propósito | Responsable |
| --- | --- | --- | --- | --- |
| **Bronze** | `bronze.usgs_raw` (PostgreSQL) | JSONB | Zona de ingesta (landing). Almacena el payload crudo, metadatos de lote, conteo esperado/recibido, hash SHA256, estado y trazabilidad. Garantiza auditoría e idempotencia. | DAG `usgs_ingest`, DAG `usgs_backfill` |
| **Silver** | `silver.events` (PostgreSQL) | Relacional tipado | Datos limpios, normalizados y deduplicados. Tipado fuerte, sin enriquecimiento de negocio. Listos para reglas de calidad y curación. | DAG `usgs_transform_quality` |
| **Gold** | `gold.*` (PostgreSQL) | Relacional curado | Capa de consumo optimizada para analítica. Contiene `events` (enriquecido), `hourly_stats` (agregados), `quality_runs` (métricas de calidad), `daily_reports` (reportes con IA), `anomalies` (detección), `ai_runs` (trazabilidad IA). | DAG `usgs_transform_quality`, DAG `usgs_daily_report` |

### Almacenamiento Complementario

- **PostgreSQL 16**: Núcleo del lakehouse. Las tres capas del medallón (`bronze`, `silver`, `gold`) se implementan como esquemas con sus respectivas tablas.
- **MinIO (S3-compatible)**: Almacenamiento objeto complementario para artefactos/raw, buckets `bronze`/`silver`/`gold` y soporte al patrón Data Lake.
- **Metadatos Airflow**: PostgreSQL gestiona la base de metadatos de Airflow (separada de los datos de negocio).

## 3. Componentes del Sistema

| Componente | Tecnología | Función |
| --- | --- | --- |
| **Orquestación** | Apache Airflow (LocalExecutor) | Gestión de DAGs, dependencias, reintentos, backfill, logging y monitoreo de pipeline. |
| **Fuente de Datos** | USGS Earthquake API | `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson` y FDSNWS para ventanas históricas. |
| **Almacenamiento Principal (Medallón)** | PostgreSQL 16 | Núcleo del lakehouse. Implementa los esquemas `bronze`, `silver` y `gold` con tablas relacionales/JSONB, constraints, índices y control de acceso por rol. |
| **Almacenamiento Objeto (Complementario)** | MinIO (S3-compatible) | Almacenamiento objeto para artefactos, raw y soporte al patrón Data Lake. |
| **Backend/API** | FastAPI + Uvicorn | REST API, autenticación JWT, autorización RBAC, validación con Pydantic, endpoints de analítica, agente IA. |
| **Consulta** | SQL sobre PostgreSQL (con soporte DuckDB/httpfs) | Consultas desde API directamente sobre PostgreSQL; DuckDB se utiliza para ciertos flujos/analítica según configuración. |
| **Frontend** | Nginx + HTML/CSS/JS + Leaflet.js | Tablero interactivo, mapa geoespacial, filtros, reportes CSV/PDF, chatbot IA. |
| **IA Local** | Ollama + Qwen 2.5 Instruct (7B) | Traducción lenguaje natural → SQL con validación estricta, solo sobre esquema `gold`. |
| **Contenedorización** | Docker, Docker Compose | Despliegue aislado, orquestación de servicios, volúmenes persistentes. |

## 4. Diagrama de Flujo de Datos Detallado

### 4.1 Flujo End-to-End

```text
┌─────────────────────────┐
│ USGS API                │
│ all_hour.geojson / FDSNWS│
└─────────────┬───────────┘
              │ HTTP GET
              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Apache Airflow                                                               │
│ ├─ usgs_ingest (cada ~5 min)                                                 │
│ ├─ usgs_backfill (manual/parametrizado)                                       │
│ ├─ usgs_transform_quality (transformación + reglas de calidad)               │
│ └─ usgs_daily_report (diario - reporte con IA)                               │
└─────────────┬───────────────────────────────────────────┬─────────────────┘
              │ Guardado de artefactos/parquets              │ Carga a capas
              ▼                                                 ▼
        ┌─────────────────┐                        ┌─────────────────────────┐
        │ MinIO           │                        │ POSTGRESQL 16            │
        │ S3-compatible   │                        │ Arquitectura Medallón    │
        │ Buckets:        │                        │ bronze → silver → gold   │
        │ bronze/silver/gold│                       │ (fuente del modelo SQL)  │
        └─────────┬───────┘                        └───────────┬─────────────┘
                  │ Artifacts / Parquet                        │
                  │ (soporte Data Lake)                        │
                  └───────────────────┬────────────────────────┘
                                      │
                                      ▼
                            ┌─────────────────────────────────────────────┐
                            │ CAPA DE CONSUMO                               │
                            │ ┌─────────┐   ┌───────────────────────────┐ │
                            │ │ FastAPI │──►│ PostgreSQL (schemas)      │ │
                            │ │ JWT+RBAC│   │ bronze/silver/gold        │ │
                            │ └─────┬───┘   │ (lectura/operación)       │ │
                            │       │       └─────────────┬─────────────┘ │
                            │       │                     │                 │
                            │       │ HTTP/REST           │ SQL             │
                            │       ▼                     ▼                 │
                            │ ┌─────────────────┐   ┌───────────────────┐ │
                            │ │ Nginx + Web UI  │   │ Ollama + Qwen 2.5 │ │
                            │ │ Dashboard/Mapa  │   │ NL → SQL          │ │
                            │ │ Reportes/Chatbot│   │ (solo SELECT gold)│ │
                            │ └─────────────────┘   └───────────────────┘ │
                            └─────────────────────────────────────────────┘
```

## 5. Modelo de Datos (Esquemas)

### Bronze - `bronze.usgs_raw`
| Campo | Tipo | Descripción |
| --- | --- | --- |
| `id` | BIGSERIAL PK | Identificador único del lote |
| `source` | TEXT | Origen: `feed_hour` o `query_window` |
| `window_start`/`window_end` | TIMESTAMPTZ | Ventana temporal del lote (NULL para feed horario) |
| `fetched_at` | TIMESTAMPTZ | Timestamp de obtención |
| `expected_count` | INT | Conteo reportado por USGS |
| `received_count` | INT | Conteo real recibido |
| `sha256` | TEXT | Hash del payload para trazabilidad/idempotencia |
| `payload` | JSONB | GeoJSON completo tal como se recibió |
| `status` | TEXT | `pending` \| `processed` \| `failed` |
| `error` | TEXT | Mensaje de error si aplica |
| `processed_at` | TIMESTAMPTZ | Timestamp de procesamiento |
| **Restricciones** | - | `CHECK (expected_count = received_count)`, índice único por (source, ventana, sha256), índice parcial en `pending` |

### Silver - `silver.events`
| Campo | Tipo | Descripción |
| --- | --- | --- |
| `event_id` | TEXT PK | ID único del evento USGS |
| `event_time` | TIMESTAMPTZ | Momento del sismo |
| `updated_at` | TIMESTAMPTZ | Última actualización en USGS |
| `magnitude` | NUMERIC(4,2) | Magnitud |
| `mag_type` | TEXT | Tipo de magnitud (ml, mb, mw, etc.) |
| `place` | TEXT | Descripción del lugar |
| `longitude`/`latitude` | DOUBLE PRECISION | Coordenadas geográficas |
| `depth_km` | DOUBLE PRECISION | Profundidad en km |
| `tsunami` | SMALLINT | Indicador de tsunami |
| `significance` | INT | Índice de significancia |
| `status` | TEXT | Estado USGS |
| `event_type` | TEXT | Tipo de evento |
| `url` | TEXT | URL al detalle USGS |
| `bronze_id` | BIGINT FK | Referencia al lote bronze.usgs_raw |
| `loaded_at` | TIMESTAMPTZ | Timestamp de carga a silver |

### Gold - Tablas Principales
| Tabla | Clave | Descripción |
| --- | --- | --- |
| `gold.events` | `event_id` PK | Eventos curados con `mag_bucket`, `region`, enriquecidos para consumo. Índices en `event_time`, `loaded_at`, `region`. |
| `gold.hourly_stats` | `hour` PK | Agregados por hora: `events`, `max_magnitude`, `avg_magnitude`. |
| `gold.quality_runs` | `id` BIGSERIAL PK | Métricas de calidad por ejecución: `rules_evaluated/passed/failed`, `score`, `status`, `checks` (JSONB), trazabilidad por `bronze_id`/`dag_id`. |
| `gold.daily_reports` | `report_date` PK | Reportes diarios generados por IA o fallback: `headline`, `summary`, `highlights`, `facts` (JSONB), `generated_by`, `model`, `prompt_version`. |
| `gold.anomalies` | `hour` PK | Detección de anomalías horarias con `baseline`, `zscore`, `explanation`. |
| `gold.ai_runs` | `id` BIGSERIAL PK | Trazabilidad de ejecuciones IA: `task`, `model`, `prompt_version`, `tokens_in/out`, `latency_ms`, `cost_usd`, `status`, `error`. |

## 6. Seguridad y Control de Acceso

### 6.1 Autenticación y Autorización
| Aspecto | Implementación |
| --- | --- |
| **Autenticación** | JWT (HS256). Token firmado con `JWT_SECRET`, expiración configurable vía `JWT_MINUTES`. |
| **Autorización** | RBAC con roles `admin` y `viewer`. Validación por endpoint/acción. |
| **Endpoints sensibles** | Protegidos con `require_admin` (acciones administrativas: backfill/regenerar, detalles de pipeline). |

### 6.2 Privilegios por Usuario de BD
| Usuario | Esquemas | Permisos | Restricciones |
| --- | --- | --- | --- |
| `ingest_user` | `bronze` | SELECT, INSERT en `bronze.usgs_raw`; USAGE en esquema/secuencia | Solo escritura/lectura en capa Bronze |
| `transform_user` | `bronze`, `silver`, `gold` | SELECT en `bronze.usgs_raw`; UPDATE (status,error,processed_at) en `bronze.usgs_raw`; SELECT/INSERT/UPDATE/DELETE en `silver`, `gold`; USAGE en secuencias | Lógica de transformación/calidad entre capas |
| `reader_user` (API/IA) | `gold` | SELECT únicamente en todas las tablas de `gold` | `default_transaction_read_only=on`, `statement_timeout='8s'`, sin acceso a bronze/silver |

### 6.3 Seguridad del Agente IA
| Control | Descripción |
| --- | --- |
| **Alcance** | Únicamente esquema `gold`. Sin acceso a `bronze`, `silver`, metadatos o sistema. |
| **Operaciones** | Solo sentencias `SELECT`. Bloqueo de DDL/DML, `DROP`, `TRUNCATE`, `UPDATE`, `DELETE`, `INSERT`, `CREATE`, etc. |
| **Validación** | Validación sintáctica, restricción por tablas/columnas permitidas, límite de filas. |
| **Timeout** | Aplicación de timeouts para prevenir consultas largas. |
| **Trazabilidad** | Todas las interacciones registradas en `gold.ai_runs`. |

## 7. Configuración y Variables de Entorno

| Variable | Propósito | Valor por defecto/Ejemplo |
| --- | --- | --- |
| `DB_NAME` | Nombre de base de datos PostgreSQL | `sismowatch` |
| `POSTGRES_PASSWORD` | Contraseña de superusuario postgres | Definido en `.env` |
| `AIRFLOW_META_PASSWORD` | Contraseña para metadatos Airflow | Definido en `.env` |
| `INGEST_PASSWORD`/`TRANSFORM_PASSWORD`/`READER_PASSWORD` | Contraseñas por rol DB | Definido en `.env` |
| `AIRFLOW_ADMIN_USER`/`AIRFLOW_ADMIN_PASSWORD` | Acceso a Airflow UI | Definido en `.env` |
| `JWT_SECRET`/`JWT_MINUTES` | Configuración JWT | Definido en `.env` (480 min) |
| `ADMIN_USER`/`ADMIN_PASSWORD`/`VIEWER_USER`/`VIEWER_PASSWORD` | Credenciales plataforma | Definido en `.env` |
| `OLLAMA_MODEL` | Modelo IA local | `qwen2.5:7b-instruct` |
| `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` | Credenciales MinIO | `minio` / `minio123` |
| `MINIO_BUCKET_BRONZE`/`SILVER`/`GOLD` | Nombres de buckets | `bronze` / `silver` / `gold` |
| `CORS_ORIGINS` | Orígenes permitidos CORS | `http://localhost:3000` |

## 8. URLs de Acceso por Defecto

| Servicio | URL | Notas |
| --- | --- | --- |
| **Web UI** | [http://localhost:3000](http://localhost:3000) | Dashboard, mapa, reportes, chatbot IA |
| **API REST + Swagger** | [http://localhost:8000/docs](http://localhost:8000/docs) | Documentación interactiva OpenAPI |
| **Apache Airflow** | [http://localhost:8086](http://localhost:8086) | Orquestación y monitoreo de DAGs |
| **MinIO Console** | [http://localhost:19001](http://localhost:19001) | Administración de buckets S3-compatible |
| **MinIO API** | [http://localhost:19000](http://localhost:19000) | Endpoint S3-compatible |

## 9. Operación y Despliegue

### Inicio Rápido
```bash
./start
```

### DAGs Principales
| DAG | Frecuencia/Tipo | Función |
| --- | --- | --- |
| `usgs_ingest` | Programado (cada ~5 min) | Ingesta de `all_hour.geojson` desde USGS a Bronze |
| `usgs_transform_quality` | Depende de ingest / programado | Transforma Bronze→Silver, aplica calidad, publica a Gold |
| `usgs_backfill` | Manual/parametrizado | Carga histórica por ventanas con validación de completitud |
| `usgs_daily_report` | Diario/programado | Genera reporte diario con IA y registra métricas en `gold.ai_runs` |

### Monitoreo y Trazabilidad
- **Calidad**: Cada ejecución registrada en `gold.quality_runs` con score y checks detallados.
- **IA**: Todas las llamadas al agente registradas en `gold.ai_runs` (modelo, tokens, latencia, costo, estado).
- **Pipeline**: Estado por lote rastreable en `bronze.usgs_raw` (`pending/processed/failed`) con timestamps.
- **Logs**: Logs de contenedores vía Docker, logs de Airflow en volumen `airflow_logs`.
