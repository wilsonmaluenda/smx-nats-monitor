import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from . import db, nats_client

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app):
    await db.init_db()
    nc = await nats_client.connect()
    poll_task = asyncio.create_task(nats_client.poll_loop(nc))
    adv_task = asyncio.create_task(nats_client.advisory_loop(nc))
    app.state.nc = nc
    app.state.tasks = (poll_task, adv_task)
    yield
    poll_task.cancel()
    adv_task.cancel()
    try:
        await nc.close()
    except Exception:
        pass


app = FastAPI(title="SMX NATS Monitor", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/state")
async def state():
    return {
        "streams": await db.get_streams(),
        "consumers": await db.get_consumers(),
    }


@app.get("/api/snapshots")
async def snapshots(entity_type: str | None = None, entity_id: str | None = None, minutes: int = 60):
    return {"snapshots": await db.get_snapshots(entity_type, entity_id, minutes)}


@app.get("/api/events")
async def events(limit: int = 200):
    return {"events": await db.get_events(limit)}


@app.get("/api/events/stream")
async def events_stream():
    q: asyncio.Queue = asyncio.Queue()
    nats_client.sse_queues.add(q)

    async def gen():
        try:
            while True:
                try:
                    event = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"data: {json.dumps(event, default=str)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            nats_client.sse_queues.discard(q)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
