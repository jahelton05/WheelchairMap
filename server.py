import asyncio
import json
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

# --- GLOBAL STATE ---
simulation_active = False  # False = Wait for Jetson, True = Read testdata.txt

# --- WEBSOCKET CONNECTION MANAGER ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections[:]:
            try:
                await connection.send_text(message)
            except Exception:
                self.disconnect(connection)

manager = ConnectionManager()

# --- BACKGROUND TEST DATA LOOP ---
async def read_testdata_loop():
    """Reads testdata.txt line-by-line ONCE when simulation_active is enabled."""
    global simulation_active
    file_path = "testdata.txt"

    while True:
        if simulation_active:
            if os.path.exists(file_path):
                try:
                    with open(file_path, "r") as f:
                        lines = f.readlines()

                    for line in lines:
                        # Check if user turned off simulation mid-run
                        if not simulation_active:
                            break

                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue

                        parts = [p.strip() for p in line.split(",")]
                        if len(parts) >= 2:
                            x = float(parts[0])
                            y = float(parts[1])
                            heading = float(parts[2]) if len(parts) >= 3 else 0.0

                            payload = json.dumps({
                                "type": "location",
                                "source": "simulation",
                                "x": x,
                                "y": y,
                                "heading": heading
                            })
                            await manager.broadcast(payload)

                            # Speed of playback (1 second per coordinate step)
                            await asyncio.sleep(1.0)

                except Exception as e:
                    print(f"Error reading {file_path}: {e}")

            # Once file reading finishes (or if file doesn't exist), turn off simulation mode
            if simulation_active:
                simulation_active = False
                await manager.broadcast(json.dumps({
                    "type": "mode_change",
                    "simulation_active": False
                }))
        else:
            # Idle sleep when simulation is inactive
            await asyncio.sleep(0.2)

# Server lifecycle manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(read_testdata_loop())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

# --- API ENDPOINT: TOGGLE SIMULATION MODE ---
@app.post("/api/simulation")
async def toggle_simulation(data: dict):
    global simulation_active
    simulation_active = bool(data.get("active", False))

    # Broadcast state update to all open web clients
    await manager.broadcast(json.dumps({
        "type": "mode_change",
        "simulation_active": simulation_active
    }))
    return {"status": "success", "simulation_active": simulation_active}

# --- API ENDPOINT: RECEIVE LIVE JETSON DATA ---
@app.post("/api/location")
async def receive_jetson_location(data: dict):
    global simulation_active

    # Process Jetson coordinates only if simulation is NOT active
    if not simulation_active:
        x = data.get("x", 0.0)
        y = data.get("y", 0.0)
        heading = data.get("heading", 0.0)

        payload = json.dumps({
            "type": "location",
            "source": "jetson",
            "x": x,
            "y": y,
            "heading": heading
        })
        await manager.broadcast(payload)
        return {"status": "success", "source": "jetson"}

    return {"status": "ignored", "reason": "Simulation mode currently active"}

# --- WEBSOCKET ENDPOINT ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)

    # Send current simulation status upon connecting
    await websocket.send_text(json.dumps({
        "type": "mode_change",
        "simulation_active": simulation_active
    }))

    try:
        while True:
            await websocket.receive_text()  # Keep connection alive
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Serve static files (index.html, evansmap.png)
app.mount("/", StaticFiles(directory=".", html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
