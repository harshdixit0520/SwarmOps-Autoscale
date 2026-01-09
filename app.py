import docker
import json
import asyncio
import logging
import math
import os
import subprocess
import requests
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from prometheus_api_client import PrometheusConnect
from pydantic import BaseModel
from typing import Dict, Optional

# --- CONFIG ---
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
SETTINGS_FILE = "/data/settings.json"
COMPOSE_PROJECT = os.getenv("COMPOSE_PROJECT_NAME", "swarm-autoscaler")

# --- SETUP ---
app = FastAPI()
templates = Jinja2Templates(directory="templates")
client = docker.from_env()
prom = PrometheusConnect(url=PROMETHEUS_URL, disable_ssl=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("SwarmOps")

# --- SWARM DETECTION ---
if os.getenv("FORCE_LOCAL_MODE") == "true":
    IS_SWARM = False
else:
    try:
        IS_SWARM = client.swarm.attrs and len(client.swarm.attrs) > 0
    except:
        IS_SWARM = False

logger.info(f"🚀 MODE: {'SWARM CLUSTER' if IS_SWARM else 'LOCAL COMPOSE'}")

# --- PERSISTENCE ---
def load_settings() -> Dict[str, dict]:
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, 'r') as f:
                return json.load(f)
        except: return {}
    return {}

def save_settings(data):
    os.makedirs(os.path.dirname(SETTINGS_FILE), exist_ok=True)
    with open(SETTINGS_FILE, 'w') as f:
        json.dump(data, f, indent=4)

# --- METRICS ---
def get_metric_value(query):
    try:
        result = prom.custom_query(query)
        if result and result[0]['value']:
            val = float(result[0]['value'][1])
            return val if val > 0.01 else 0.0
    except Exception as e:
        return 0.0
    return 0.0

def get_cpu_metric(service_name):
    if IS_SWARM:
        query = f'avg(rate(container_cpu_usage_seconds_total{{container_label_com_docker_swarm_service_name="{service_name}"}}[1m])) * 100'
    else:
        query = f'avg(rate(container_cpu_usage_seconds_total{{name=~".*{service_name}.*"}}[1m])) * 100'
    return get_metric_value(query)

def get_memory_metric(service_name):
    # Logic: (Usage / Limit) * 100
    if IS_SWARM:
        # We sum usage of all replicas and divide by sum of limits of all replicas to get Avg %
        query = f'(sum(container_memory_usage_bytes{{container_label_com_docker_swarm_service_name="{service_name}"}}) / sum(container_spec_memory_limit_bytes{{container_label_com_docker_swarm_service_name="{service_name}"}})) * 100'
    else:
        query = f'(sum(container_memory_usage_bytes{{name=~".*{service_name}.*"}}) / sum(container_spec_memory_limit_bytes{{name=~".*{service_name}.*"}})) * 100'
    
    val = get_metric_value(query)
    # Fallback: If no limit is set (0), return 0 to avoid DivisionByZero or Infinity errors
    return val if val < 1000 else 0.0 

def get_replica_count(service_name):
    try:
        if IS_SWARM:
            return client.services.get(service_name).attrs['Spec']['Mode']['Replicated']['Replicas']
        else:
            count = 0
            for c in client.containers.list():
                if service_name in c.name or c.labels.get('com.docker.compose.service') == service_name:
                    count += 1
            return count
    except: return 0

def scale_target(service_name, replicas):
    if IS_SWARM:
        try:
            client.services.get(service_name).scale(replicas)
        except Exception as e:
            logger.error(f"❌ Scale Failed {service_name}: {e}")
    else:
        try:
            cmd = ["docker", "compose", "-p", COMPOSE_PROJECT, "scale", f"{service_name}={replicas}"]
            subprocess.run(cmd, check=True)
        except: pass

def get_all_services():
    services = set()
    try:
        if IS_SWARM:
            for s in client.services.list(): services.add(s.name)
        else:
            for c in client.containers.list():
                name = c.labels.get('com.docker.compose.service', c.name)
                services.add(name)
    except: pass
    return list(services)

async def autoscaler_loop():
    logger.info("✅ Autoscaler Loop Started")
    while True:
        try:
            settings = load_settings()
            services = get_all_services()
            
            if not services:
                await asyncio.sleep(5)
                continue

            for name in services:
                if name not in settings or not settings[name].get('enabled'): continue
                
                config = settings[name]
                curr = get_replica_count(name)
                cpu = get_cpu_metric(name)
                
                target = float(config.get('target', 40))
                
                # Currently only scaling on CPU, but now we LOG memory too
                if cpu > 0 and target > 0:
                    ratio = cpu / target
                    new_count = math.ceil(curr * ratio)
                    
                    mn = int(config.get('min', 1))
                    mx = int(config.get('max', 10))
                    new_count = max(mn, min(mx, int(new_count)))
                    
                    if new_count != curr:
                        logger.info(f"⚡ SCALING {name}: {curr} -> {new_count} (CPU: {cpu:.1f}%)")
                        scale_target(name, new_count)
                        
        except Exception as e:
            logger.error(f"Loop Error: {e}")
            
        await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(autoscaler_loop())

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/data")
async def get_data():
    settings = load_settings()
    data = []
    try:
        services = sorted(get_all_services())
        for name in services:
            conf = settings.get(name, {"enabled": False, "min": 1, "max": 10, "target": 40})
            display_name = name.replace("swarm_", "").replace("otp_", "")
            data.append({
                "name": name,
                "display": display_name,
                "replicas": get_replica_count(name),
                "cpu": round(get_cpu_metric(name), 1),
                "memory": round(get_memory_metric(name), 1), # ADDED MEMORY HERE
                "config": conf
            })
    except Exception as e:
        logger.error(f"API Error: {e}")
    return JSONResponse(data)

class ConfigUpdate(BaseModel):
    enabled: bool
    min: int
    max: int
    target: float

@app.post("/api/update/{service_name}")
async def update_service(service_name: str, config: ConfigUpdate):
    settings = load_settings()
    settings[service_name] = config.dict()
    save_settings(settings)
    return {"status": "ok"}
