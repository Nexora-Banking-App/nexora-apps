from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
import math

app = FastAPI()
Instrumentator().instrument(app).expose(app)

@app.get("/health/liveness")
def liveness(): return {"status": "alive"}

@app.get("/health/readiness")
def readiness(): return {"status": "ready"}

@app.get("/api/scan")
def cpu_heavy_task():
    # We use this to artificially spike CPU for our Kubernetes Auto-scaling demo.
    x = 0.0001
    for _ in range(500000):  
        x += math.sqrt(x)
    
    return {"status": "scanned", "threat_level": "low", "cpu_cycles_used": "high"}