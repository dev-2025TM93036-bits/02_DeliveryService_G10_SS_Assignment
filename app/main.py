import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import Depends, FastAPI, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, create_engine, func
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker

SERVICE_NAME = os.getenv("SERVICE_NAME", "delivery-service")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./delivery.db")

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(SERVICE_NAME)
REQUEST_COUNT = Counter("http_requests_total", "Total HTTP requests", ["service", "path", "method", "status_code"])
REQUEST_LATENCY = Histogram("http_request_duration_seconds", "Request latency", ["service", "path", "method"])
ASSIGNMENT_LATENCY_MS = Gauge("delivery_assignment_latency_ms", "Delivery assignment latency", ["service"])


class Base(DeclarativeBase):
    pass


class Driver(Base):
    __tablename__ = "drivers"
    driver_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(150))
    phone: Mapped[str] = mapped_column(String(20))
    vehicle_type: Mapped[str] = mapped_column(String(40))
    is_active: Mapped[bool] = mapped_column(Boolean)
    deliveries: Mapped[list["Delivery"]] = relationship(back_populates="driver")


class Delivery(Base):
    __tablename__ = "deliveries"
    delivery_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(Integer, unique=True)
    driver_id: Mapped[int] = mapped_column(ForeignKey("drivers.driver_id"))
    status: Mapped[str] = mapped_column(String(20))
    assigned_at: Mapped[datetime] = mapped_column(DateTime)
    picked_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    driver: Mapped[Driver] = relationship(back_populates="deliveries")


engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class ApiError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 400):
        self.code = code
        self.message = message
        self.status_code = status_code


class AssignRequest(BaseModel):
    order_id: int
    city: str


class DeliveryStatusUpdate(BaseModel):
    status: str


class DriverOut(BaseModel):
    driver_id: int
    name: str
    phone: str
    vehicle_type: str
    is_active: bool

    class Config:
        from_attributes = True


class DeliveryOut(BaseModel):
    delivery_id: int
    order_id: int
    driver_id: int
    status: str
    assigned_at: datetime
    picked_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None

    class Config:
        from_attributes = True


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def seed_data():
    with SessionLocal() as db:
        if db.query(Driver).first():
            return
        db.add_all([
            Driver(driver_id=1, name="Driver One", phone="9000000001", vehicle_type="BIKE", is_active=True),
            Driver(driver_id=2, name="Driver Two", phone="9000000002", vehicle_type="SCOOTER", is_active=True),
            Driver(driver_id=3, name="Driver Three", phone="9000000003", vehicle_type="BIKE", is_active=True),
            Driver(driver_id=4, name="Driver Four", phone="9000000004", vehicle_type="BICYCLE", is_active=False),
        ])
        db.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    seed_data()
    yield


app = FastAPI(title="Delivery Service", version="1.0.0", lifespan=lifespan)


@app.middleware("http")
async def telemetry(request: Request, call_next):
    correlation_id = request.headers.get("X-Correlation-Id", str(uuid.uuid4()))
    request.state.correlation_id = correlation_id
    started = time.perf_counter()
    response = await call_next(request)
    elapsed = time.perf_counter() - started
    REQUEST_COUNT.labels(SERVICE_NAME, request.url.path, request.method, str(response.status_code)).inc()
    REQUEST_LATENCY.labels(SERVICE_NAME, request.url.path, request.method).observe(elapsed)
    response.headers["X-Correlation-Id"] = correlation_id
    logger.info(json.dumps({"service": SERVICE_NAME, "correlationId": correlation_id, "path": request.url.path, "statusCode": response.status_code, "latencyMs": round(elapsed * 1000, 2)}))
    return response


@app.exception_handler(ApiError)
async def api_error_handler(request: Request, exc: ApiError):
    return JSONResponse(status_code=exc.status_code, content={"code": exc.code, "message": exc.message, "correlationId": getattr(request.state, "correlation_id", str(uuid.uuid4()))})


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST)


@app.get("/v1/drivers", response_model=list[DriverOut])
def list_drivers(is_active: Optional[bool] = None, db: Session = Depends(get_db)):
    query = db.query(Driver)
    if is_active is not None:
        query = query.filter(Driver.is_active == is_active)
    return query.all()


@app.get("/v1/deliveries", response_model=list[DeliveryOut])
def list_deliveries(status: Optional[str] = None, limit: int = Query(10, ge=1, le=100), offset: int = Query(0, ge=0), db: Session = Depends(get_db)):
    query = db.query(Delivery)
    if status:
        query = query.filter(Delivery.status == status)
    return query.order_by(Delivery.delivery_id.desc()).offset(offset).limit(limit).all()


@app.post("/v1/deliveries/assign")
def assign_delivery(payload: AssignRequest, db: Session = Depends(get_db)):
    existing = db.query(Delivery).filter(Delivery.order_id == payload.order_id).first()
    if existing:
        return {"delivery_id": existing.delivery_id, "driver_id": existing.driver_id, "status": existing.status, "idempotentReplay": True}
    started = time.perf_counter()
    driver = db.query(Driver).outerjoin(Delivery).filter(Driver.is_active == True).group_by(Driver.driver_id).order_by(func.count(Delivery.delivery_id).asc(), Driver.driver_id.asc()).first()
    if not driver:
        raise ApiError("NO_DRIVER_AVAILABLE", "No active driver available", 409)
    delivery = Delivery(order_id=payload.order_id, driver_id=driver.driver_id, status="ASSIGNED", assigned_at=datetime.utcnow())
    db.add(delivery)
    db.commit()
    db.refresh(delivery)
    ASSIGNMENT_LATENCY_MS.labels(SERVICE_NAME).set(round((time.perf_counter() - started) * 1000, 2))
    return {"delivery_id": delivery.delivery_id, "driver_id": delivery.driver_id, "status": delivery.status, "city": payload.city}


@app.patch("/v1/deliveries/{delivery_id}")
def update_delivery_status(delivery_id: int, payload: DeliveryStatusUpdate, db: Session = Depends(get_db)):
    delivery = db.get(Delivery, delivery_id)
    if not delivery:
        raise ApiError("DELIVERY_NOT_FOUND", f"Delivery {delivery_id} not found", 404)
    new_status = payload.status.upper()
    allowed = {"ASSIGNED": ["PICKED"], "PICKED": ["DELIVERED"], "DELIVERED": []}
    if new_status not in allowed.get(delivery.status, []):
        raise ApiError("INVALID_DELIVERY_TRANSITION", f"Cannot move from {delivery.status} to {new_status}", 409)
    delivery.status = new_status
    now = datetime.utcnow()
    if new_status == "PICKED":
        delivery.picked_at = now
    elif new_status == "DELIVERED":
        delivery.delivered_at = now
    db.commit()
    return {"delivery_id": delivery.delivery_id, "status": delivery.status}
