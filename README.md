# 01 Test task for Engineer

lunch

1.  `docker compose up --build`
2.  go to swager `http://0.0.0.0:8000/docs#/default/get_filtered_projects_price_post`
3.  endpoint also post("/price")

Notes / assumptions:
• The database is hydrated on every start, coz a time limit.
• On each start, all rows from the parser are inserted into the database.

TODOS:
t1) parce_pdf("../FedEx_Standard_List_Rates_2025.pdf"): -> bool
parce

t2) normolize_service(zone: str): -> string
services = [First Overnight, Priority Overnight, Standard Overnight, 2Day, 2Day AM, Express Saver, Ground, Home Delivery]

t3) normolize_weight(string of number) -> interger
decapitalize all with reg.
weight = [5 lb, 5 lbs, 6ld, 6lbs]
return вес с округлением

t4) normolize_zone(string of number) -> string
decapitalize all with reg.
zones = [zone 5, zone5, z5]

t4) parce_string(string of number) -> []
strip string with reg
return normolize_delivery_options(), normolize_zone() ,normolize_weight()

t4) make get_price(line: str) -> Decimal|float
#main fn
delivery_option ,zone, weight = parce_string(line)
search from db base.Tarifs.find_one({delivery_option ,zone, weight},{"\_id":0})

t5) @app.post(/price)
async def get_filtered_projects(request: FilterRequest):
line = request.line
if not line:
raise HTTPException(status_code=400, detail="Input string cannot be empty")
return get_price(line)

t6) dockerize
