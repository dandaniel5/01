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

import pandas as pd
import pdfplumber

MONGO_HOST = os.environ.get("MONGO_HOST", "localhost")
MONGO_PORT = os.environ.get("MONGO_PORT", "27017")
MONGO_USERNAME = os.environ.get("MONGO_USERNAME", "root")
MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD", "example")

MONGO_URI = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}"
# MONGO_URI = "mongodb://root:example@127.0.0.1:27017"
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
    page: int

class FilterRequest(BaseModel):
    line: str


async def get_price(line: str) -> Optional[float]:
    
    service = normalize_service(line)
    zone = normalize_zone(line)
    weight = normalize_weight(line)

    print("service", service)
    print("zone", zone)
    print("weight", weight)

    if not (service and zone and weight):
        return None

    doc = await tarifs.find_one(
        {"service": service, "zone": zone, "weight": weight},
        {"_id": 0, "base_price": 1},
    )
    return doc["base_price"] if doc else None



# ------------------------------------------
# Вспомогательные нормализации
# ------------------------------------------


def normalize_zone(text: str) -> Tuple[Optional[str], bool]:
    """
    Извлекает зону из текста заголовка и возвращает:
    - zone (str или None)
    - has_to (bool) — True, если в ключевой фразе было 'to'
    """
    if not text:
        return None, False

    text = text.strip()
    has_to = False

    # Ключевые фразы
    patterns = [
        (r"U\.S\. package rates to\s*(.*)", True),
        (r"U\.S\. package rates:\s*(.*)", False)
    ]

    for pattern, to_flag in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            zone_text = m.group(1).strip()
            return zone_text, to_flag

    # Если не нашли, ищем "Zone <номер>"
    m = re.search(r"Zone\s*(\d+)", text, re.IGNORECASE)
    if m:
        return m.group(1).strip(), False

    # Возвращаем исходный текст и False, если ничего не найдено
    return text, False

def normalize_weight(raw: str):
    """Парсим вес, возвращаем список чисел"""
    raw = raw.strip().lower()
    m = re.match(r"(\d+)", raw)
    if m:
        return [int(m.group(1))]
    return None

def parse_price(raw: str):
    """Парсим цену"""
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
]

def normalize_service(text: str):
    """
    Приводим название сервиса к ключу:
    - убираем переносы строк, лишние пробелы
    - убираем символы ® и @
    - оставляем только реальные типы FedEx
    """
    if not text:
        return None

    # чистим текст: переносы строк, табуляции, множественные пробелы
    text = re.sub(r'\s+', ' ', text.lower()).strip()
    text = text.replace("®", "").replace("@", "").strip()

    # ищем совпадение с валидными сервисами
    for service in VALID_SERVICES:
        if service in text:
            # возвращаем корректное название с title
            return service.title()

    # если строка не валидный сервис — игнорируем
    return None


# ------------------------------------------
# Основной парсер
# ------------------------------------------

VALID_RANGES = [(1, 26)] 

def is_valid_weight(weight: int) -> bool:
    """Проверяем, попадает ли вес в допустимые диапазоны"""
    for start, end in VALID_RANGES:
        if start <= weight <= end:
            return True
    return False

async def parse_pdf(file_path):
    all_records = []
    with pdfplumber.open(file_path) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            if is_valid_weight(page_idx):
                page_text = page.extract_text()
                tables = page.extract_tables()
                
                if page_text:
                    zone, has_to = normalize_zone(page_text)
                    
                  
                    
                    # Если ключевая фраза содержала "to", берём номер зоны из таблицы
                    if tables and has_to:
                        for first_table in tables:
                            if len(first_table) > 1:  # минимум 2 строки
                                # проверяем вторую строку
                                second_row = first_table[1]

                                hard_zone_name = None

                                # пробуем взять -5 элемент
                                if len(second_row) >= 5 and second_row[-5]:
                                    hard_zone_name = second_row[-5].strip()
                                # если нет, пробуем -3 элемент
                                elif len(second_row) >= 3 and second_row[-3]:
                                    hard_zone_name = second_row[-3].strip()

                                # если нашли, присваиваем zone
                                if hard_zone_name:
                                    zone = hard_zone_name
                    
                    print("zone   ", zone )
                    print("service_name", service_name)     
                    
                    
                    if tables and not has_to:
                        for table in tables:
                            service_names = []

                            
                            header_row = table[1]
                            
                            for cell in header_row:
                                if cell and ("@" in cell or "®" in cell):
                                    
                                    # service_name = normalize_service(cell)
                                    # Service ={
                                    #     name: service_name,
                                    #     prices: []
                                    # }
                                    #     проходим по таблице 
                                    #     создаем массив 
                                    #     число из 0 стобца i строки : число из index стобца i строки
                                    #         каждый проход делаем апенд prices
                                        
                                    # if service_name:
                                    #     print("service_name", service_name)
                                    #     service_names.append(service_name)
                                    
                
                    
                            # if not service_names:
                            #     continue  # если услуг не нашли, пропускаем таблицу

                            # # теперь идём по строкам с весами и ценами
                            # for row in table[1:]:  # пропускаем шапку
                            #     if not row or len(row) < 2:
                            #         continue

                            #     weight_data = normalize_weight(str(row[0]))
                            #     if not weight_data:
                            #         continue

                            #     weight = weight_data[0]

                            #     # фильтруем по диапазону весов, если нужно
                            #     if not is_valid_weight(weight):
                            #         continue

                            #     # остальные колонки — цены
                            #     prices = [parse_price(c) for c in row[1:] if c]
                            #     for i, price in enumerate(prices):
                            #         if price is None:
                            #             continue
                            #         service = service_names[i % len(service_names)]
                            #         all_records.append({
                            #             "service": service,
                            #             "zone": zone or "N/A",
                            #             "weight": weight,
                            #             "price": price,
                            #             "page": page_idx + 1
                            #         })

                        # tables = page.extract_tables()
                        # if tables:
                        #     for table in tables:
                        #         # Delivery commitment3  
                        #         for row in table:
                        #             first_cell = row[0]
                        #             if first_cell:
                        #                 if "delivery commitment" in first_cell.lower():
                        #                     # новый блок, очищаем текущие сервисы
                        #                     current_block_services = []
                        #                     continue

                        #                     # пробуем нормализовать сервис из первой колонки
                        #                     service_name = normalize_service(first_cell)
                        #                     if service_name:
                        #                         # нашли реальный сервис, добавляем в текущий блок
                        #                         current_block_services.append(service_name)

                        #                     # если блок с сервисами найден, парсим веса и цены
                        #                     if current_block_services:
                        #                         weights_data = normalize_weight(str(row[0]))
                        #                         if not weights_data:
                        #                             continue

                        #                         # остальные колонки — цены
                        #                         prices = [parse_price(c) for c in row[1:] if c]
                        #                         for i, price in enumerate(prices):
                        #                             if price is None:
                        #                                 continue
                        #                             service = current_block_services[i % len(current_block_services)]
                        #                             all_records.append({
                        #                                 "service": service,
                        #                                 "zone": zone or "N/A",
                        #                                 "weight": weights_data[0],
                        #                                 "price": price,
                        #                                 "page": page_idx + 1
                        #                             })

        if all_records:
            await tarifs.insert_many(all_records)
        print(f"✅ Загружено {len(all_records)} тарифов")
        return pd.DataFrame(all_records)



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
