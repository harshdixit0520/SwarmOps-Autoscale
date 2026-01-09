import docker
import json
import asyncio
import logging
import math
import os
import time
from datetime import datetime, timedelta
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
SCALING_COOLDOWN = int(os.getenv("SCALING_COOLDOWN", "30"))  # seconds between scaling actions

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# --- DOCKER CLIENT SETUP ---
client = docker.from_env()
api_client = docker.APIClient(base_url='unix://var/run/docker.sock')
prom = PrometheusConnect(url=PROMETHEUS_URL, disable_ssl=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("SwarmAutoscaler")

# --- SWARM CHECK ---
if os.getenv("FORCE_LOCAL_MODE") == "true":
    IS_SWARM = False
else:
    try:
        IS_SWARM = client.swarm.attrs and len(client.swarm.attrs) > 0
    except:
        IS_SWARM = False

logger.info(f"🚀 MODE: {'SWARM CLUSTER' if IS_SWARM else 'LOCAL COMPOSE'}")

# Track last scaling time per service
last_scale_time = {}

# --- PERSISTENCE ---
def load_settings() -> Dict[str, dict]:
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, 'r') as f: 
                return json.load(f)
        except: 
            return {}
    return {}

def save_settings(data):
    os.makedirs(os.path.dirname(SETTINGS_FILE), exist_ok=True)
    with open(SETTINGS_FILE, 'w') as f: 
        json.dump(data, f, indent=4)

# --- METRICS ---
def get_metric_value(query):
    """Execute Prometheus query and return first value"""
    try:
        result = prom.custom_query(query)
        if result and len(result) > 0 and 'value' in result[0]:
            val = float(result[0]['value'][1])
            return val if val > 0.01 else 0.0
    except Exception as e:
        logger.debug(f"Metric query failed: {query[:100]}... Error: {e}")
        return 0.0
    return 0.0

def get_cpu_metric(service_name):
    """Get aggregated CPU usage across all replicas (not average per replica)"""
    if IS_SWARM:
        # Sum CPU across all containers of this service
        query = f'sum(rate(container_cpu_usage_seconds_total{{container_label_com_docker_swarm_service_name="{service_name}"}}[1m])) * 100'
    else:
        query = f'sum(rate(container_cpu_usage_seconds_total{{name=~".*{service_name}.*"}}[1m])) * 100'
    return get_metric_value(query)

def get_memory_metric(service_name):
    """Get aggregated memory usage across all replicas"""
    if IS_SWARM:
        # Average memory percentage across replicas
        query = f'avg((container_memory_usage_bytes{{container_label_com_docker_swarm_service_name="{service_name}"}} / container_spec_memory_limit_bytes{{container_label_com_docker_swarm_service_name="{service_name}"}}) * 100)'
    else:
        query = f'avg((container_memory_usage_bytes{{name=~".*{service_name}.*"}} / container_spec_memory_limit_bytes{{name=~".*{service_name}.*"}}) * 100)'
    
    val = get_metric_value(query)
    return val if val < 1000 else 0.0

def get_service_mode(service_name):
    """Check if service is replicated or global"""
    try:
        if IS_SWARM:
            service = client.services.get(service_name)
            mode = service.attrs['Spec']['Mode']
            if 'Replicated' in mode:
                return 'replicated', mode['Replicated'].get('Replicas', 0)
            elif 'Global' in mode:
                # For global services, count running tasks
                tasks = api_client.tasks(filters={'service': service_name, 'desired-state': 'running'})
                return 'global', len(tasks)
    except:
        pass
    return 'unknown', 0

def get_replica_count(service_name):
    """Get replica count or task count for global services"""
    mode, count = get_service_mode(service_name)
    return count

def scale_service(service_name, replicas):
    """Scale service with cooldown protection"""
    if not IS_SWARM:
        return False
    
    # Check cooldown
    now = time.time()
    if service_name in last_scale_time:
        elapsed = now - last_scale_time[service_name]
        if elapsed < SCALING_COOLDOWN:
            logger.debug(f"⏳ Cooldown active for {service_name} ({int(SCALING_COOLDOWN - elapsed)}s remaining)")
            return False
    
    try:
        service = client.services.get(service_name)
        mode, current = get_service_mode(service_name)
        
        if mode != 'replicated':
            logger.warning(f"⚠️ Cannot scale {service_name}: service is in {mode} mode")
            return False
        
        service.scale(replicas)
        last_scale_time[service_name] = now
        logger.info(f"✅ Scaled {service_name}: {current} → {replicas}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Scale failed for {service_name}: {e}")
        return False

def get_all_services():
    """Get list of all Swarm services"""
    services = []
    try:
        if IS_SWARM:
            for s in client.services.list():
                services.append(s.name)
    except Exception as e:
        logger.error(f"Failed to list services: {e}")
    return services

# --- SYSTEM STATS ---
def get_system_stats():
    """Get cluster-wide statistics"""
    stats = {
        "nodes": [], 
        "info": {"stacks": 0, "networks": 0, "volumes": 0, "containers": 0, "services": 0}
    }
    
    if IS_SWARM:
        try:
            # Get all running tasks
            all_tasks = api_client.tasks(filters={'desired-state': 'running'})
            running_tasks = [t for t in all_tasks if t.get('Status', {}).get('State') == 'running']
            
            # Count tasks per node
            node_task_map = {}
            for t in running_tasks:
                node_id = t.get('NodeID')
                if node_id:
                    node_task_map[node_id] = node_task_map.get(node_id, 0) + 1

            # Get node information
            nodes = client.nodes.list()
            for n in nodes:
                nid = n.id
                stats["nodes"].append({
                    "name": n.attrs['Description']['Hostname'],
                    "role": n.attrs['Spec']['Role'],
                    "status": n.attrs['Status']['State'],
                    "addr": n.attrs['Status'].get('Addr', 'N/A'),
                    "containers": node_task_map.get(nid, 0)
                })
            
            # Get services, networks, volumes
            services = client.services.list()
            networks = client.networks.list()
            volumes = client.volumes.list()
            
            # Count unique stacks
            stacks = set()
            for s in services:
                labels = s.attrs.get('Spec', {}).get('Labels', {})
                stack_label = labels.get('com.docker.stack.namespace')
                if stack_label:
                    stacks.add(stack_label)

            stats["info"] = {
                "stacks": len(stacks),
                "networks": len(networks),
                "volumes": len(volumes),
                "containers": len(running_tasks),
                "services": len(services)
            }
        except Exception as e:
            logger.error(f"System stats error: {e}")
            
    return stats

# --- AUTOSCALING LOGIC ---
def calculate_desired_replicas(service_name, current_replicas, config):
    """
    Calculate desired replica count based on CPU and Memory thresholds
    Returns: (desired_replicas, reason)
    """
    cpu = get_cpu_metric(service_name)
    memory = get_memory_metric(service_name)
    
    cpu_target = float(config.get('cpu_target', 70))
    mem_target = float(config.get('memory_target', 80))
    min_replicas = int(config.get('min', 1))
    max_replicas = int(config.get('max', 10))
    
    # Calculate desired replicas based on CPU
    desired_cpu = current_replicas
    if cpu > 0 and cpu_target > 0 and current_replicas > 0:
        # Scale up if CPU is above target, scale down if below
        cpu_ratio = cpu / cpu_target
        desired_cpu = math.ceil(current_replicas * cpu_ratio)
    
    # Calculate desired replicas based on Memory
    desired_mem = current_replicas
    if memory > 0 and mem_target > 0 and current_replicas > 0:
        mem_ratio = memory / mem_target
        desired_mem = math.ceil(current_replicas * mem_ratio)
    
    # Take the maximum to ensure we have enough capacity for both CPU and Memory
    desired = max(desired_cpu, desired_mem)
    
    # Apply min/max constraints
    desired = max(min_replicas, min(max_replicas, desired))
    
    # Determine scaling reason
    reason = ""
    if desired > current_replicas:
        if cpu > cpu_target:
            reason = f"CPU {cpu:.1f}% > {cpu_target}%"
        elif memory > mem_target:
            reason = f"MEM {memory:.1f}% > {mem_target}%"
    elif desired < current_replicas:
        if cpu < cpu_target and memory < mem_target:
            reason = f"CPU {cpu:.1f}% < {cpu_target}%, MEM {memory:.1f}% < {mem_target}%"
    
    return desired, reason

# --- AUTOSCALER LOOP ---
async def autoscaler_loop():
    """Main autoscaling loop"""
    logger.info("✅ Autoscaler loop started")
    await asyncio.sleep(10)  # Wait for Prometheus to collect initial data
    
    while True:
        try:
            settings = load_settings()
            services = get_all_services()
            
            for service_name in services:
                # Skip if not configured or not enabled
                if service_name not in settings:
                    continue
                    
                config = settings[service_name]
                if not config.get('enabled', False):
                    continue
                
                # Check if service is scalable
                mode, current = get_service_mode(service_name)
                if mode != 'replicated':
                    continue
                
                # Calculate desired replicas
                desired, reason = calculate_desired_replicas(service_name, current, config)
                
                # Scale if needed
                if desired != current:
                    logger.info(f"⚡ SCALING {service_name}: {current} → {desired} ({reason})")
                    scale_service(service_name, desired)
                    
        except Exception as e:
            logger.error(f"Autoscaler loop error: {e}")
        
        await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(autoscaler_loop())

# --- API ROUTES ---
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/data")
async def get_data():
    """Get all services data and system stats"""
    settings = load_settings()
    services_data = []
    
    try:
        service_list = sorted(get_all_services())
        
        for name in service_list:
            # Get or create default config
            conf = settings.get(name, {
                "enabled": False,
                "min": 1,
                "max": 10,
                "cpu_target": 70,
                "memory_target": 80
            })
            
            # Get service mode and count
            mode, count = get_service_mode(name)
            
            # Get metrics
            cpu = get_cpu_metric(name)
            memory = get_memory_metric(name)
            
            # Clean display name
            display_name = name
            
            services_data.append({
                "name": name,
                "display": display_name,
                "replicas": count,
                "mode": mode,
                "cpu": round(cpu, 1),
                "memory": round(memory, 1),
                "config": conf
            })
    except Exception as e:
        logger.error(f"API data error: {e}")
        
    system_stats = get_system_stats()
    
    return JSONResponse({
        "services": services_data,
        "system": system_stats
    })

class ConfigUpdate(BaseModel):
    enabled: bool
    min: int
    max: int
    cpu_target: float
    memory_target: float

@app.post("/api/update/{service_name}")
async def update_service(service_name: str, config: ConfigUpdate):
    """Update service autoscaling configuration"""
    settings = load_settings()
    settings[service_name] = config.dict()
    save_settings(settings)
    logger.info(f"📝 Updated config for {service_name}: {config.dict()}")
    return {"status": "ok"}

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}
