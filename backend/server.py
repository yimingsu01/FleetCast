from fastapi import FastAPI
from satellite_config import main as simulate_and_log
import pymysql
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import os
import ssl
from dotenv import load_dotenv
from prometheus_fastapi_instrumentator import Instrumentator

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

load_dotenv()

app = FastAPI()
instrumentator = Instrumentator()
instrumentator.instrument(app).expose(app)

resource = Resource.create({"service.name": "fleetcast-backend"})

provider = TracerProvider(resource=resource)
trace.set_tracer_provider(provider)
jaeger_exporter = JaegerExporter(
    agent_host_name="jaeger-agent.observe.svc.cluster.local",
    agent_port=6831,
)

span_processor = BatchSpanProcessor(jaeger_exporter)
provider.add_span_processor(span_processor)

FastAPIInstrumentor.instrument_app(app)
RequestsInstrumentor().instrument()
tracer = trace.get_tracer(__name__)


TIDB_HOST = "basic-tidb.tidb-cluster.svc.cluster.local"
TIDB_USER = "root"
TIDB_PASSWORD = ""
TIDB_DATABASE = "satellite_sim"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "http://orbital.local:8080", "127.0.0.1:3000"],  # unchanged
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


scheduler = BackgroundScheduler()

def scheduled_simulation():
    with tracer.start_as_current_span("scheduled_simulation"):
        print("Running scheduled simulation")
        simulate_and_log()

def scheduled_dashboard_job():
    data = get_dashboard_summary()
    print("[JOB] /api/dashboard ran at", datetime.utcnow().isoformat(), "->", {k: data[k] for k in ("totalTelemetry","lowBattery","errorState","activeContacts")})

def scheduled_station_job():
    for sid in ["GS-1", "GS-2", "GS-3"]:
        data = get_station_data(sid)
        print(f"[JOB] /api/station/{sid} ran at", datetime.utcnow().isoformat(), "-> count:", len(data.get("satellites", [])))


@app.on_event("startup")
def start_jobs():
    scheduler.add_job(scheduled_simulation, IntervalTrigger(seconds=10), coalesce=True, max_instances=1)
    scheduler.add_job(scheduled_dashboard_job, IntervalTrigger(seconds=15), coalesce=True, max_instances=1)
    scheduler.add_job(scheduled_station_job, IntervalTrigger(seconds=20), coalesce=True, max_instances=1)
    scheduler.start()
    print("[SCHEDULER] started YIMING")

@app.on_event("shutdown")
def stop_jobs():
    scheduler.shutdown(wait=False)
    print("[SCHEDULER] stopped YIMING")


@app.get("/api/simulate")
def run_simulation():
    with tracer.start_as_current_span("simulate_and_log"):
        simulate_and_log()
    return {"message": "Simulation completed and data logged to TiDB"}

@app.get("/api/dashboard")
def get_dashboard_summary():
    ssl_ca_path = os.path.join(os.path.dirname(__file__), 'tidb-ca.pem')
    with tracer.start_as_current_span("fetch_dashboard_data") as span:
        conn = pymysql.connect(
            host=TIDB_HOST,
            port=4000,
            user=TIDB_USER,
            password=TIDB_PASSWORD,
            database=TIDB_DATABASE,
            ssl={
                "ca": "/app/tidb-ca.pem",
                "check_hostname": True,
                "verify_mode": ssl.CERT_REQUIRED
            }
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM telemetry")
        total_telemetry = cursor.fetchone()[0]
        span.set_attribute("db.system", "mysql")
        span.set_attribute("db.operation", "SELECT COUNT(*) FROM telemetry")

        with tracer.start_as_current_span("query_low_battery") as span:
            cursor.execute("""
                SELECT COUNT(*) FROM telemetry t
                INNER JOIN (
                    SELECT satellite_id, MAX(timestamp) AS latest_time
                    FROM telemetry
                    GROUP BY satellite_id
                ) latest ON t.satellite_id = latest.satellite_id AND t.timestamp = latest.latest_time
                WHERE t.battery_level < 30
            """)
            low_battery = cursor.fetchone()[0]
            span.set_attribute("db.system", "mysql")
            span.set_attribute("db.operation", "low_battery_latest")
            span.set_attribute("telemetry.low_battery_count", low_battery)
            #span.add_event("Low battery query executed")

        with tracer.start_as_current_span("query_error_states") as span:
            cursor.execute("""
                SELECT COUNT(*) FROM telemetry t
                INNER JOIN (
                    SELECT satellite_id, MAX(timestamp) AS latest_time
                    FROM telemetry
                    GROUP BY satellite_id
                ) latest ON t.satellite_id = latest.satellite_id AND t.timestamp = latest.latest_time
                WHERE t.status = 'ERROR'
            """)
            errors = cursor.fetchone()[0]
            span.set_attribute("db.system", "mysql")
            span.set_attribute("db.operation", "error_latest")
            span.set_attribute("telemetry.error_count", errors)
            #span.add_event("Error state query executed")

        with tracer.start_as_current_span("query_active_contacts") as span:
            cursor.execute("""
                SELECT COUNT(DISTINCT satellite_id)
                FROM contact_windows
                WHERE assigned = TRUE AND end_time > UTC_TIMESTAMP()
            """)

            active_contacts = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            span.set_attribute("db.system", "mysql")
            span.set_attribute("db.operation", "active_contacts_now")
            span.set_attribute("contacts.active_count", active_contacts)
           # span.add_event("Active contacts query executed")

    return {
        "totalSatellites": 100,
        "activeContacts": active_contacts,
        "lowBattery": low_battery,
        "errorState": errors,
        "totalTelemetry": total_telemetry
    }

@app.get("/api/station/{station_id}")
def get_station_data(station_id: str):
    with tracer.start_as_current_span("get_station_data") as span:
        ssl_ca_path = os.path.join(os.path.dirname(__file__), 'tidb-ca.pem')
        conn = pymysql.connect(
            host=TIDB_HOST,
            port=4000,
            user=TIDB_USER,
            password=TIDB_PASSWORD,
            database=TIDB_DATABASE,
            ssl={
                "ca": "/app/tidb-ca.pem",
                "check_hostname": True,
                "verify_mode": ssl.CERT_REQUIRED
            }
        )
        cursor = conn.cursor()
        span.set_attribute("db.operation", "get_station_data")
        span.set_attribute("db.system", "mysql")
        span.set_attribute("station.id", station_id)
        with tracer.start_as_current_span("query_station_latest") as span:
            cursor.execute("""
                SELECT
                    sub.ground_station_id,
                    sub.satellite_id,
                    sub.battery_level,
                    sub.temperature,
                    sub.status,
                    sub.timestamp
                FROM (
                    SELECT
                        cw.ground_station_id,
                        cw.satellite_id,
                        t.battery_level,
                        t.temperature,
                        t.status,
                        t.timestamp,
                        ROW_NUMBER() OVER (
                            PARTITION BY cw.satellite_id
                            ORDER BY t.timestamp DESC
                        ) AS rn
                    FROM contact_windows cw
                    JOIN telemetry t ON cw.satellite_id = t.satellite_id
                                     AND cw.ground_station_id = t.ground_station_id
                    WHERE cw.assigned = TRUE
                      AND cw.end_time > UTC_TIMESTAMP()
                      AND cw.ground_station_id = %s
                ) sub
                WHERE sub.rn = 1
            """, (station_id,))

            results = cursor.fetchall()
            station_data = [{
                "satellite_id": row[1],
                "battery_level": row[2],
                "temperature": row[3],
                "status": row[4],
                "timestamp": row[5]
            } for row in results]
            span.set_attribute("db.system", "mysql")
            span.set_attribute("db.operation", "station_latest_telemetry")
            span.set_attribute("station.satellite_count", len(station_data))
           # span.add_event("Station data query executed")
        cursor.close()
        conn.close()

    return {"station_id": station_id, "satellites": station_data}

@app.get("/api/health")
def health_check():
    with tracer.start_as_current_span("health_check") as span:
        span.set_attribute("service.status", "healthy")
        return {"status": "ok"}

