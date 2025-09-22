# main.py
import os
import re
import math
import pdfplumber
from typing import Optional
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

# MONGO_URI = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}"
MONGO_URI = "mongodb://root:example@127.0.0.1:27017/?authSource=tarifs_db"
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


SERVICES = {
    "first overnight": "First Overnight",
    "priority overnight": "Priority Overnight",
    "standard overnight": "Standard Overnight",
    "2day": "2Day",
    "2day am": "2Day AM",
    "express saver": "Express Saver",
    "ground": "Ground",
    "home delivery": "Home Delivery",
}

# def normalize_service(line: str) -> Optional[str]:
#     line = line.lower()
#     for k, v in SERVICES.items():
#         if k in line:
#             return v
#     return None

# def normalize_weight(line: str) -> Optional[int]:
#     match = re.search(r"(\d+)", line)
#     if not match:
#         return None
#     return math.ceil(int(match.group(1)))

# def normalize_zone(line: str) -> Optional[str]:
#     match = re.search(r"z(?:one)?\s*(\d+)", line.lower())
#     if match:
#         return f"Z{match.group(1)}"
#     return None

# # async def parse_pdf(pdf_path: str = "../FedEx_Standard_List_Rates_2025.pdf") -> bool:

# #     print("parse_pdf")
# #     async for _ in tarifs.find({}):
# #         # если уже есть данные — не парсим повторно
# #         return True

# #     with pdfplumber.open(pdf_path) as pdf:
# #         for page in pdf.pages:
# #             tables = page.extract_tables()
# #             for table in tables:
# #                 for row in table:
# #                     try:
# #                         service, zone, weight, price = row
# #                         doc = {
# #                             "service": normalize_service(service) or service,
# #                             "zone": normalize_zone(zone) or zone,
# #                             "weight": normalize_weight(weight),
# #                             "base_price": float(price),
# #                         }
# #                         await tarifs.insert_one(doc)
# #                     except Exception:
# #                         continue
# #     return True

# async def parse_pdf(pdf_path: str = "./FedEx_Standard_List_Rates_2025.pdf") -> bool:
#     """
#     Парсит PDF и складывает данные в MongoDB.

#     :param pdf_path: Путь к PDF-файлу.
#     :return: True, если парсинг был выполнен или данные уже существуют.
#     """
#     print("parse_pdf: checking for existing data...")

#     # Используем find_one() для эффективной проверки наличия данных
#     if await tarifs.find_one({}):
#         print("parse_pdf: Data already exists. Skipping parsing.")
#         return True

#     print("parse_pdf: No data found. Starting PDF parsing...")

#     data_to_insert = []
#     try:
#         # Примечание: pdfplumber.open() является блокирующей операцией.
#         # В реальном приложении для больших файлов ее стоит вынести в
#         # отдельный поток, например, с помощью run_in_executor.
#         with pdfplumber.open(pdf_path) as pdf:
#             for page in pdf.pages:
#                 tables = page.extract_tables()
#                 for table in tables:
#                     for row in table:
#                         try:
#                             service, zone, weight, price = row
#                             doc = {
#                                 "service": normalize_service(service) or service,
#                                 "zone": normalize_zone(zone) or zone,
#                                 "weight": normalize_weight(weight),
#                                 "base_price": float(price),
#                             }
#                             data_to_insert.append(doc)
#                         except Exception as e:
#                             # Пропускаем строки, которые не соответствуют ожидаемому формату
#                             print(f"Error parsing row: {e} - Row: {row}")
#                             continue
#     except Exception as e:
#         print(f"parse_pdf: An error occurred during file processing: {e}")
#         return False

#     if data_to_insert:
#         try:
#             # Используем insert_many() для пакетной вставки, это намного эффективнее
#             await tarifs.insert_many(data_to_insert)
#             print(f"parse_pdf: Successfully inserted {len(data_to_insert)} documents.")
#         except Exception as e:
#             print(f"parse_pdf: An error occurred during bulk insert: {e}")
#             return False
#     else:
#         print("parse_pdf: No valid data found to insert.")

#     return True

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

def normalize_service(service):
    """Normalize service names to a consistent format."""
    if not isinstance(service, str):
        return None
    service = service.lower().replace("fedex\n", "").replace("®", "").strip()
    if "first overnight" in service:
        return "First Overnight"
    if "priority overnight" in service:
        return "Priority Overnight"
    if "standard overnight" in service:
        return "Standard Overnight"
    if "2day a.m." in service:
        return "2Day A.M."
    if "2day" in service:
        return "2Day"
    if "express saver" in service:
        return "Express Saver"
    return None

def normalize_zone(zone):
    """Normalize zone data."""
    if not isinstance(zone, str):
        return None
    cleaned_zone = zone.replace('–', '-').strip()
    if 'zone' in cleaned_zone.lower():
        cleaned_zone = cleaned_zone.lower().replace('zone', '').strip()
    if cleaned_zone.isdigit() or ('-' in cleaned_zone and all(part.isdigit() for part in cleaned_zone.split('-'))):
        return cleaned_zone
    return None

def normalize_weight(weight):
    """Normalize weight data, handling multi-line text and cleaning up suffixes."""
    if not isinstance(weight, str):
        return None
    # Split by newline and get all numbers
    weights = [w.strip().replace('lbs.', '').replace('lb.', '').replace('lbs', '').replace('lb', '')
               for w in weight.split('\n') if w.strip() and w.strip().replace('.', '', 1).isdigit()]

    # Handle weight ranges and single values
    normalized_weights = []
    for w in weights:
        if '–' in w:
            start, end = w.split('–')
            normalized_weights.extend(range(int(start), int(end) + 1))
        else:
            normalized_weights.append(float(w))

    return normalized_weights if normalized_weights else None

def parse_price(price_str):
    """Extract and clean prices from a string, which can contain multiple prices."""
    if not isinstance(price_str, str):
        return None

    # Remove all non-numeric characters except '.'
    prices = [p.replace('$', '').replace(',', '').strip() for p in price_str.split('\n') if p.strip()]

    # Convert to float and return a list
    cleaned_prices = []
    for p in prices:
        try:
            cleaned_prices.append(float(p))
        except ValueError:
            continue

    return cleaned_prices if cleaned_prices else None


async def parse_pdf(file_path):
    all_records = []
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                services = []
                # First pass to identify services
                for row in table:
                    if len(row) > 1 and isinstance(row[0], str):
                        service_name = normalize_service(row[0])
                        if service_name:
                            services.append(service_name)

                # If no services found, this is not a service rate table
                if not services:
                    continue

                for row in table:
                    # Skip rows that are clearly headers or invalid
                    if len(row) < 4 or row[0] is None or 'weight' in str(row[0]).lower():
                        continue

                    zone = normalize_zone("U.S. rates: Zone 2") # Assuming a fixed zone for simplicity from the logs
                    weights_data = normalize_weight(str(row[0]))
                    if not weights_data:
                        continue

                    # The data is structured where the first column is the weight(s) and the following columns are prices.
                    # This assumes the table format is [Weights, Price1, Price2, ...], so we pair the weights with prices.
                    prices_data = [parse_price(str(p)) for p in row[1:]]

                    # Flatten the lists and pair them with the services
                    if weights_data and prices_data:
                        all_prices = []
                        for price_list in prices_data:
                            if price_list:
                                all_prices.extend(price_list)

                        if len(weights_data) == len(all_prices):
                            for i, weight in enumerate(weights_data):
                                for j, service in enumerate(services):
                                    price_index = (len(services) * i) + j
                                    if price_index < len(all_prices):
                                        all_records.append({
                                            'service': service,
                                            'zone': zone,
                                            'weight': float(weight),
                                            'price': all_prices[price_index]
                                        })
    print("all_records", all_records)
    return pd.DataFrame(all_records)

@app.get("/")
async def greetings():
    return {"hellow":"api is ok"}

class FilterRequest(BaseModel):
    line: str

@app.post("/price")
async def get_filtered_projects(request: FilterRequest):
    line = request.line
    if not line:
        raise HTTPException(status_code=400, detail="Input string cannot be empty")

    price = await get_price(line)
    if price is None:
        raise HTTPException(status_code=404, detail="Price not found")

    return {"line": line, "price": price}
