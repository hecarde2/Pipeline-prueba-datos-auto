# Arquitectura SismoWatch

## Visión General

SismoWatch implementa un **data lakehouse** con arquitectura medallón sobre **MinIO**. Los datos sísmicos de USGS fluyen desde bronze a silver a gold, se consultan con DuckDB desde la API y se visualizan en un tablero profesional con asistente IA local.

## Capas Medallón

| Capa | Formato | Propósito |
| --- | --- | --- |
| **Bronze** | JSON + Parquet (manifiesto) | Datos crudos tal como llegan de USGS. Trazabilidad y auditoría. |
| **Silver** | Parquet | Datos limpios, tipados y normalizados. Listos para transformación. |
| **Gold** | Parquet | Datos curados para consumo: `events`, `hourly_stats`, `quality_runs`. |

## Componentes

| Componente | Tecnología | Función |
| --- | --- | --- |
| Orquestación | Apache Airflow | Ingesta, transformación, calidad y reportes |
| Almacenamiento | MinIO (S3) | Buckets bronze/silver/gold |
| Backend | FastAPI | API REST, autenticación JWT, agente IA |
| Consulta | DuckDB + httpfs | Consultas SQL sobre parquet remoto |
| Frontend | Nginx + HTML/JS | Tablero interactivo con mapa y chatbot |
| IA Local | Ollama + Qwen | Traducción NL → SQL con validación estricta |

## Flujo de Datos

1. **Ingesta** (`usgs_ingest`): Cada 5 minutos obtiene `all_hour.geojson` desde USGS, guarda en bronze con manifiesto.
2. **Transformación + Calidad** (`usgs_transform_quality`): Lee manifests pendientes, normaliza a silver, aplica reglas de calidad y publica a gold.
3. **Backfill** (`usgs_backfill`): Carga histórica por ventanas con validación de completitud vía `/count`.
4. **Reporte Diario** (`usgs_daily_report`): Genera resumen con IA y guarda en gold.

## Seguridad

- **Autenticación**: JWT (HS256), expiración configurable via `JWT_MINUTES`
- **Autorización**: Roles `admin` y `visita` con control por endpoint
- **Agente IA**: Solo `SELECT`, únicamente sobre esquema `gold`, límite de filas
- **RBAC**: Endpoints sensibles protegidos con `require_admin`

## URLs por Defecto

| Servicio | URL |
| --- | --- |
| Web | http://localhost:3000 |
| API Docs | http://localhost:8000/docs |
| Airflow | http://localhost:8086 |
| MinIO Console | http://localhost:19001 |

## Inicio

```bash
./start
```
