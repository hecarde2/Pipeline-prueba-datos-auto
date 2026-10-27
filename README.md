# SismoWatch

Plataforma de monitoreo sísmico con arquitectura **medallón** (Bronze / Silver / Gold) sobre **MinIO (S3-compatible)**, orquestada con **Apache Airflow**, API **FastAPI** con **JWT + roles**, frontend profesional y **agente IA local** con **Ollama + Qwen**.

## Arquitectura

```text
USGS (all_hour.geojson / fdsnws/event/1/query)
                ↓
            Airflow DAGs
                ↓
        Bronze (JSON crudo + manifiesto)
                ↓
            Transform + Calidad
                ↓
        Silver (parquet tipado/limpio)
                ↓
            Publish Gold
                ↓
        Gold (events, hourly_stats, quality_runs)
                ↓
          FastAPI (DuckDB + httpfs)
                ↓
              Nginx + Web UI
                ↓
              Ollama Agent (NL → SQL SELECT)
```

## Servicios

| Servicio | Puerto | Descripción |
| --- | --- | --- |
| Web | `3000` | Frontend estático servido por Nginx |
| API | `8000` | FastAPI (Swagger en `/docs`) |
| Airflow | `8086` | Orquestación de pipelines |
| MinIO API | `19000` | Almacenamiento S3-compatible |
| MinIO Console | `19001` | Consola web de MinIO |
| Ollama | interno | Modelo IA local |

## Puesta en marcha

```bash
./start
```

El script valida `.env`, levanta todos los servicios con `docker compose` y muestra las URLs.

## Uso rápido

1. Abrir [http://localhost:3000](http://localhost:3000)
2. Iniciar sesión con credenciales de `.env` (`ADMIN_USER` / `ADMIN_PASSWORD` o `VIEWER_USER` / `VIEWER_PASSWORD`)
3. Abrir [http://localhost:8086](http://localhost:8086) y activar los DAGs: `usgs_ingest`, `usgs_transform_quality`, `usgs_backfill`, `usgs_daily_report`
4. Ejecutar `usgs_backfill` con `{"days": 1}` para cargar datos de prueba
5. Recargar el tablero para ver KPIs, mapa, eventos y usar el chatbot IA

## Roles

| Rol | Acceso |
| --- | --- |
| `viewer` | Dashboard, reportes (CSV/PDF), tiempo real, pipeline (resumen) |
| `admin` | Todo lo anterior + detalle de pipeline, acciones admin (backfill/regenerar) |

## Tecnologías

- **Almacenamiento**: MinIO
- **Formato**: Parquet (PyArrow)
- **Consulta**: DuckDB con `httpfs`
- **Orquestación**: Apache Airflow
- **Backend**: FastAPI + Uvicorn
- **Frontend**: HTML/CSS/JS + Leaflet
- **IA**: Ollama + Qwen 2.5 Instruct
