import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated

import asyncpg
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, Field
from redis.asyncio import Redis

DATABASE_URL = os.environ["DATABASE_URL"]
REDIS_URL = os.environ["REDIS_URL"]
QUEUE = "student_registrations"
EVENTS = "student_events"

pool: asyncpg.Pool | None = None
redis: Redis | None = None


class StudentRegistration(BaseModel):
    name: Annotated[str, Field(min_length=2, max_length=120)]
    email: EmailStr
    course: Annotated[str, Field(min_length=2, max_length=120)]
    phone: Annotated[str, Field(min_length=7, max_length=30)]


class Student(StudentRegistration):
    id: int
    created_at: datetime


class Connections:
    def __init__(self) -> None:
        self.items: set[WebSocket] = set()

    async def send(self, students: list[dict]) -> None:
        message = json.dumps(students, default=str)
        for websocket in list(self.items):
            try:
                await websocket.send_text(message)
            except Exception:
                self.items.discard(websocket)


connections = Connections()


async def students() -> list[dict]:
    assert pool is not None
    rows = await pool.fetch("SELECT id, name, email, course, phone, created_at FROM students ORDER BY created_at DESC, id DESC")
    return [dict(row) for row in rows]


async def listen_for_events() -> None:
    assert redis is not None
    subscriber = redis.pubsub()
    await subscriber.subscribe(EVENTS)
    try:
        async for message in subscriber.listen():
            if message["type"] == "message":
                await connections.send(await students())
    finally:
        await subscriber.close()


@asynccontextmanager
async def lifespan(_: FastAPI):
    global pool, redis
    pool = await asyncpg.create_pool(DATABASE_URL)
    await pool.execute("""
        CREATE TABLE IF NOT EXISTS students (
            id BIGSERIAL PRIMARY KEY,
            name VARCHAR(120) NOT NULL,
            email VARCHAR(320) NOT NULL UNIQUE,
            course VARCHAR(120) NOT NULL,
            phone VARCHAR(30) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    listener = asyncio.create_task(listen_for_events())
    try:
        yield
    finally:
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)
        await redis.close()
        await pool.close()


app = FastAPI(title="School Service", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/students", status_code=202)
async def register_student(registration: StudentRegistration) -> dict[str, str]:
    assert redis is not None
    try:
        await redis.rpush(QUEUE, registration.model_dump_json())
    except Exception as error:
        raise HTTPException(status_code=503, detail="Registration queue unavailable") from error
    return {"status": "queue"}


@app.get("/students", response_model=list[Student])
async def list_students() -> list[dict]:
    try:
        return await students()
    except Exception as error:
        raise HTTPException(status_code=503, detail="Student database unavailable") from error


@app.websocket("/ws/students")
async def student_updates(websocket: WebSocket) -> None:
    await websocket.accept()
    connections.items.add(websocket)
    try:
        await websocket.send_text(json.dumps(await students(), default=str))
        while True:
            await websocket.receive_text()
    except (WebSocketDisconnect, Exception):
        connections.items.discard(websocket)