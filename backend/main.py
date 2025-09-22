# main.py
import os
import re
import math
import pdfplumber
from typing import Optional, List, Tuple
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import AsyncMongoClient
from pymongo import ASCENDING

import pandas as pd
import pdfplumber

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
        await parse_pdf(file_path = "./FedEx_Standard_List_Rates_2025.pdf")
        print("parsed well")

    except Exception as e:
        print(f"Error during startup: {str(e)}")
        raise e

    yield

    print("Buy,buy")

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



async def get_price(line: str) -> Optional[float]:
    service_name = normalize_service(line)
    print("service", service_name)
    
    zone = normalize_zone(line)
    print("zone", zone)
    
    weight_list = normalize_weight(line)
    if not weight_list:
        return None
    weight = weight_list[0]
    print("weight (original)", weight)

    if not (service_name and zone and weight):
        return None

    zone_doc = await tarifs.find_one({"area_zone": zone}, {"_id": 0, "services": 1})
    if not zone_doc:
        return None

    service_doc = None
    for s in zone_doc["services"]:
        if normalize_service(s["name"]) == service_name:
            service_doc = s
            break
    if not service_doc:
        return None


    prices = service_doc.get("prices", [])
    if not prices:
        return None

    available_weights = sorted(p["weight"] for p in prices)
    target_weight = next((w for w in available_weights if w >= weight), None)
    if target_weight is None:
        return None

    price_entry = next((p for p in prices if p["weight"] == target_weight), None)
    return price_entry["price"] if price_entry else None

# ------------------------------------------
# Вспомогательные нормализации
# ------------------------------------------


def parse_zone(text: str) -> Tuple[Optional[str], bool]:
    """
    Извлекает зону из текста заголовка и возвращает:
    - zone (str или None)
    - has_to (bool) — True, если в ключевой фразе было 'to'
    """
    if not text:
        return None, False

    text = text.strip()
    has_to = False

    patterns = [
        (r"U\.S\. package rates to\s*(.*)", True),
        (r"U\.S\. package rates:\s*(.*)", False)
    ]

    for pattern, to_flag in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            zone_text = m.group(1).strip()

            if not to_flag:
                return zone_text.split()[1], to_flag
            return zone_text, to_flag

    m = re.search(r"Zone\s*(\d+)", text, re.IGNORECASE)
    if m:
        return m.group(1).strip(), False

    return text, False


def normalize_weight(raw: str) -> Optional[List[int]]:
    """Парсим вес, возвращаем список чисел. Берем последнее число в строке."""
    if not raw:
        return None
    raw = raw.strip().lower()
    numbers = re.findall(r"\d+", raw)
    if numbers:
        return [int(numbers[-1])]
    return None


def normalize_zone(raw: str) -> Optional[int]:
    """Парсим номер зоны из строки."""
    if not raw:
        return None
    raw = raw.strip().lower()
    m = re.search(r"(?:z|zone)\s*(\d+)", raw)
    if m:
        return int(m.group(1))
    return None


def parse_weight(text: str) -> Optional[int]:
    if not text:
        return None

    m = re.search(r"(\d+)", text)
    return int(m.group(1)) if m else None


def parse_price(raw: str):
    """Парсим цену"""
    if raw:
        raw = raw.replace("$", "").replace(",", "").strip()
        try:
            return float(raw)
        except ValueError:
            return None



VALID_SERVICES = [
    "first overnight",
    "priority overnight",
    "standard overnight",
    "2day am",
    "2day",
    "express saver",
    "ground",
    "home delivery"
]

VALID_SERVICES.sort(key=len, reverse=True)

def normalize_service(text: str):
    if not text:
        return None

    text = re.sub(r'\s+', ' ', text.lower()).strip()
    text = text.replace("®", "").replace("@", "").replace(".", "").strip()

    for service in VALID_SERVICES:
        if service in text:
            return service.title()
    return None



VALID_RANGES = [(1, 26)] 

def is_valid_weight(weight: int) -> bool:
    """Проверяем, попадает ли вес в допустимые диапазоны"""
    for start, end in VALID_RANGES:
        if start <= weight <= end:
            return True
    return False


async def save_zones_to_db_bulk(zones: List[Zone]):
    zone_dicts = [zone.dict() for zone in zones]
    if zone_dicts:
        result = await tarifs.insert_many(zone_dicts)



def parse_services(table: List[List[str]], zone_number: int, has_to: bool) -> Zone:
    services = []
    if has_to:
        header = table[3] 
        service_indices = {
            idx: cell.strip()
            for idx, cell in enumerate(header)
            if cell 
            and ("@" in cell or "®" in cell)
            and not cell.strip().startswith("FedEx®\nEnvelope")
        }


        for col_idx, service_name in service_indices.items():
            prices = []
            real_service_name = normalize_service(service_name)

            for row in table[2:]:
                if not row:
                    continue
                weight_cell = row[1] or row[0]
                price_cell = row[col_idx] if col_idx < len(row) else None
                if not weight_cell or not price_cell:
                    continue

                weights = [parse_weight(w) for w in re.split(r"[\s\n]+", weight_cell) if parse_weight(w) is not None]
                price_parts = [parse_price(p) for p in re.split(r"[\s\n]+", price_cell) if parse_price(p) is not None]

                price_iter = iter(price_parts)
                for weight in weights:
                    try:
                        price = next(price_iter)
                        prices.append({"weight": weight, "price": price})
                
                    except StopIteration:
                        break

            services.append({"name": service_name, "prices": prices})

        pass
    else:
        header = table[1]
        service_indices = {
            idx: cell.strip()
            for idx, cell in enumerate(header)
            if cell 
            and ("@" in cell or "®" in cell)
            and not cell.strip().startswith("FedEx®\nEnvelope")
        }

    
        for col_idx, service_name in service_indices.items():
            prices = []
            real_service_name = normalize_service(service_name)

            for row in table[2:]:
                if not row:
                    continue
                weight_cell = row[1] or row[0]
                price_cell = row[col_idx] if col_idx < len(row) else None
                if not weight_cell or not price_cell:
                    continue

                weights = [parse_weight(w) for w in re.split(r"[\s\n]+", weight_cell) if parse_weight(w) is not None]
                price_parts = [parse_price(p) for p in re.split(r"[\s\n]+", price_cell) if parse_price(p) is not None]

                price_iter = iter(price_parts)
                for weight in weights:
                    try:
                        price = next(price_iter)
                        prices.append({"weight": weight, "price": price})
                
                    except StopIteration:
                        break 

            services.append({"name": service_name, "prices": prices})

    return services


async def parse_pdf(file_path):
    all_records = []

    with pdfplumber.open(file_path) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            if is_valid_weight(page_idx):
                page_text = page.extract_text()
                tables = page.extract_tables()

                if page_text:
                    zone, has_to = parse_zone(page_text)
                    if has_to:
                        for first_table in tables:
                            
                            if len(first_table) > 1:
                                second_row = first_table[1]

                                hard_zone_name = None
                                if len(second_row) >= 5 and second_row[-5]:
                                    hard_zone_name = second_row[-5].strip()
                                elif len(second_row) >= 3 and second_row[-3]:
                                    hard_zone_name = second_row[-3].strip()
                                
                                zone_obj = {
                                    "area_zone": hard_zone_name,
                                    "services": []
                                }

                    else:
                        zone_obj = {
                                "area_zone": int(zone),
                                "services": []
                            }

                    zones = []
                    for i, table in enumerate(tables, start=1):
                        
                        servicesservices = parse_services(table, zone_number=zone_obj["area_zone"], has_to=has_to)
                        
                        zone_obj["services"] = servicesservices

                        zone_doc = await tarifs.find_one({"area_zone": zone_obj["area_zone"]}, {"_id": 0, "services": 1})
                        if not zone_doc:
                            print("zone_obj", zone_obj)
                            if zone_obj["area_zone"] and zone_obj["services"]:

                                new_tarif = await tarifs.insert_one({"area_zone": zone_obj["area_zone"], "services": zone_obj["services"]})


                        if zone_doc:
                            existing_services = zone_doc.get("services", [])
                            new_services = []

                            for service in zone_obj["services"]:
                                existing_service = next((s for s in existing_services if s["name"] == service["name"]), None)

                                if not existing_service:
                                    new_services.append(service)
                                else:
                                    existing_prices = existing_service.get("prices", [])
                                    new_prices = []

                                    for price_entry in service.get("prices", []):
                                        if not any(p["weight"] == price_entry["weight"] for p in existing_prices):
                                            new_prices.append(price_entry)

                                    if new_prices:
                                        await tarifs.update_one(
                                            {"area_zone": zone_obj["area_zone"], "services.name": service["name"]},
                                            {"$push": {"services.$.prices": {"$each": new_prices}}}
                                        )
                                        print(f"Added {len(new_prices)} new price entries to service {service['name']} in zone {zone_obj['area_zone']}")

                            if new_services:
                                await tarifs.update_one(
                                    {"area_zone": zone_obj["area_zone"]},
                                    {"$push": {"services": {"$each": new_services}}}
                                )
                                print(f"Added {len(new_services)} new services to zone {zone_obj['area_zone']}")


@app.get("/")
async def greetings():
    return {"hellow":"api is ok"}


@app.post("/price")
async def get_filtered_projects(request: FilterRequest):
    line = request.line
    if not line:
        raise HTTPException(status_code=400, detail="Input string cannot be empty")

    price = await get_price(line)
    if price is None:
        raise HTTPException(status_code=404, detail="Price not found")

    return {"line": line, "price": price}
