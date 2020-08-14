Get("http://worldtimeapi.org/api/ip")
ResponseType("json")
LoadStrategy(Incremental, primary_key='datetime')

TableDefinition({
  "datetime": "TIMESTAMP",
  "day_of_week": "INT",
  "day_of_year": "INT",
  "week_number": "INT",
  "dst": "BOOLEAN",
  "client_ip": "VARCHAR(16)",
  "timezone": "VARCHAR(32)",
})

def Paginate(previousResponse):
  return None

def Transform(response):
  return {
    key: response[key]
    for key in ["datetime", "day_of_week", "day_of_year", "week_number", "dst", "client_ip", "timezone"]
  }
