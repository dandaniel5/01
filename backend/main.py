import os
import re

from typing import Optional, List
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import AsyncMongoClient


MONGO_HOST = os.environ.get("MONGO_HOST", "localhost")
MONGO_PORT = os.environ.get("MONGO_PORT", "27017")
MONGO_USERNAME = os.environ.get("MONGO_USERNAME", "root")
MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD", "example")

MONGO_URI = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}"
DB_NAME = "tarifs_db"

client = AsyncMongoClient(MONGO_URI)
db = client[DB_NAME]
tarifs = db["tarifs"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        print("Hello, hello")
        await hydrate_db(file_path="./data.csv")
        print("db hydrated ✅")

    except Exception as e:
        print(f"Error during startup: {str(e)} 😱")
        raise e

    yield

    print("Buy, buy")

app = FastAPI(lifespan=lifespan)


class PriceEntry(BaseModel):
    weight: int
    price: float

class Service(BaseModel):
    name: str
    prices: List[PriceEntry]

class Zone(BaseModel):
    area_zone: int
    services: List[Service]

class FilterRequest(BaseModel):
    line: str



# ------------------------------------------
# Вспомогательные нормализации
# ------------------------------------------


import re
from typing import Optional

def normalize_weight(raw: str) -> Optional[int]:
    """
    Парсим вес из строки запроса.
    Берём число, которое идёт перед lb или lbs.
    Если вес не указан, возвращаем None.
    """
    if not raw:
        return None

    raw = raw.lower()
    m = re.search(r"(\d+)\s*(lb|lbs)", raw, re.IGNORECASE)
    if m:
        return int(m.group(1))

    return None


def normalize_zone(raw: str) -> Optional[int]:
    """Парсим номер зоны из запроса и нормализуем его."""
    if not raw:
        return None
    raw = raw.strip().lower()
    m = re.search(r"(?:z|zone)\s*(\d+)", raw)
    if m:
        return int(m.group(1))
    return None


async def get_all_weights():
    """Возвращает все уникальные веса из коллекции tarifs."""
    cursor = tarifs.find({}, {"services.prices.weight": 1, "_id": 0})
    docs = await cursor.to_list(length=None)

    all_weights = set()
    for doc in docs:
        for service in doc.get("services", []):
            for price_entry in service.get("prices", []):
                w = price_entry.get("weight")
                if w is not None:
                    all_weights.add(w)

    return sorted(all_weights)



async def get_all_services():
    cursor = tarifs.find({}, {"services.name": 1, "_id": 0})
    docs = await cursor.to_list(length=None)

    all_services = set()
    for doc in docs:
        for service in doc.get("services", []):
            name = service.get("name")
            if name:
                clean_name = name.lower().replace("fedex", "").replace('"', '').strip()
                if clean_name:
                    all_services.add(clean_name)
    return list(all_services)

async def normalize_service(text: str, all_services: list) -> Optional[str]:
    if not text:
        return None

    text = re.sub(r'\s+', ' ', text.lower()).strip()
    text = text.replace("®", "").replace("@", "").replace(".", "").strip()

    if "2day" in text:
        if "am" in text or "a m" in text:
            return "2day a.m."
        else:
            return "2day"

    stop_words = {"and", "the", "&"}

    for service in all_services:
        service_words = [
            w for w in service.replace(".", "").lower().split() if w not in stop_words
        ]
        if any(word in text for word in service_words):
            return service

    return None


async def hydrate_db(file_path: str):
    await tarifs.drop()
    zones: List[Zone] = []

    with open(file_path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("Zone"):
            zone_num = int(line.split(":")[0].split()[1])
            header = lines[i + 1].split(";")
            service_names = header[1:]
            services = [Service(name=name, prices=[]) for name in service_names]

            j = i + 2
            while j < len(lines) and not lines[j].startswith("Zone"):
                row = lines[j].split(";")
                if row[0].lower() == "weight" or not row[0].isdigit():
                    j += 1
                    continue

                weight = int(row[0])
                prices = row[1:]
                for k in range(min(len(prices), len(services))):
                    try:
                        price = float(prices[k])
                        services[k].prices.append(PriceEntry(weight=weight, price=price))
                    except ValueError:
                        continue
                j += 1

            zones.append(Zone(area_zone=zone_num, services=services))
            i = j
        else:
            i += 1

    docs = [zone.dict() for zone in zones]
    if docs:
        await tarifs.insert_many(docs)

@app.get("/")
async def greetings():
    return {"hellow":"api is ok"}


async def get_price(service_name: str, zone: int, weight: int) -> Optional[float]:
    """
    Получаем цену по названию сервиса, зоне и весу.
    service_name: очищенное название сервиса без 'fedex' и лишних пробелов.
    zone: номер зоны
    weight: целое число веса
    """
    # Ищем документ с нужной зоной
    doc = await tarifs.find_one({"area_zone": zone})
    if not doc:
        return None  # зона не найдена

    # Ищем нужный сервис
    for service in doc.get("services", []):
        # Сравниваем с очищенным названием сервиса
        clean_name = service.get("name", "").lower().replace("fedex", "").replace('"', '').strip()
        if clean_name == service_name.lower():
            # Ищем нужный вес
            for price_entry in service.get("prices", []):
                if price_entry.get("weight") == weight:
                    return price_entry.get("price")
            return None  # вес не найден в этом сервисе
    return None  # сервис не найден  


@app.post("/price")
async def get_filtered_projects(request: FilterRequest):
    line = request.line
    error_answer = {}
    if not line:
        error_answer["error"] = "Input string cannot be empty"

   

    zone = normalize_zone(line)
    
    if zone:
        cursor = tarifs.find({}, {"area_zone": 1, "_id": 0})
        dbs_zone_docs = await cursor.to_list(length=None)
        zones_numbers = [z["area_zone"] for z in dbs_zone_docs]


        if zone not in zones_numbers:
            error_answer["zone"] = {"error": f"zone{zone} not found in data, avalable zones: {zones_numbers}" }
    else:
        cursor = tarifs.find({}, {"area_zone": 1, "_id": 0})
        dbs_zone_docs = await cursor.to_list(length=None)
        zones_numbers = [z["area_zone"] for z in dbs_zone_docs]
        error_answer["zone"] = {"error": f"cannot normalize zone {zone}, avalable zones: {zones_numbers}" }

    all_services = await get_all_services()
    service_name = await normalize_service(line, all_services)
    if service_name:
        if service_name not in all_services:
            error_answer["service"] = {"error": f"service {service_name} not found in data, avalable services: {all_services}" }
    else:
        error_answer["service"] = {"error": f"cannot normalize service, avalable services: {all_services}" }

    

    weight = normalize_weight(line)
    all_weights = await get_all_weights()

    if weight:
        if weight not in all_weights:
            error_answer["weight"] = {
                "error": f"weight {weight} not found in data, available weights: {all_weights}"
            }
    else:
        error_answer["weight"] = {
            "error": f"cannot normalize weight, available weights: {all_weights}"
        }

    if error_answer:
        return {"error": 204, "message": error_answer}

    if not (zone and service_name and weight):
        return {"error": 202, "message": "34404"}

    price = await get_price(service_name, zone, weight)

    if price:
        return {"price": price}
    else:
        return  {"error": 202, "message": "some other error"}
