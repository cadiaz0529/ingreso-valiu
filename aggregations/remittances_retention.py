from pymongo import MongoClient

MONGO_HOST = os.environ["MONGO_HOST"]
MONGO_PORT = os.environ["MONGO_PORT"]
MONGO_USER = os.environ["MONGO_INITDB_ROOT_USERNAME"]
MONGO_PASSWORD = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
MONGO_DB = os.environ["MONGO_INITDB_DATABASE"]

client = MongoClient(f'mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/')
db = client[MONGO_DB]
remits = db["remits"]

usuarios_activos = remits.aggregate([
    {
        "$lookup": {
            "from": "remits",
            "pipeline": [
                {
                    "$group": {
                        "_id": None,
                        "maxDate": {"$max": "$Created At"}
                    }
                },
                {
                    "$project": {
                        "maxDate": 1
                    }
                }
            ],
            "as": "filteredDate"
        }
    },
    {
        "$unwind": "$filteredDate"
    },
    {
        "$match": {
            "Currency Src": "COP",
            "Currency Dst": "VES",
            "Status": "completed",
            "$expr": { "$gte": ["$Created At", {"$subtract": ["$filteredDate.maxDate", 1000*3600*24*7*28]}] }
        }
    },
    {
        "$group": {
            "_id": "$User Processor"
        }
    },
    {
        "$project": {
            "_id": 0,
            "user_id": "$_id"
        }
    }
])

usuarios_activos_iniciales = remits.aggregate([
    {
        "$lookup": {
            "from": "remits",
            "pipeline": [
                {
                    "$group": {
                        "_id": None,
                        "maxDate": {"$max": "$Created At"}
                    }
                },
                {
                    "$project": {
                        "maxDate": 1
                    }
                }
            ],
            "as": "filteredDate"
        }
    },
    {
        "$unwind": "$filteredDate"
    },
    {
        "$match": {
            "Currency Src": "COP",
            "Currency Dst": "VES",
            "Status": "completed",
            "$expr": { "$lt": ["$Created At", {"$subtract": ["$filteredDate.maxDate", 1000*3600*24*7*28]}] }
        }
    },
    {
        "$group": {
            "_id": "$User Processor"
        }
    },
    {
        "$project": {
            "_id": 0,
            "user_id": "$_id"
        }
    }
])

usuarios_activos = [x["user_id"] for x in usuarios_activos]
usuarios_activos_iniciales = [x["user_id"] for x in usuarios_activos_iniciales]

def get_retention_rate():
    retenidos = 0
    for user in usuarios_activos:
        if user in usuarios_activos_iniciales:
            retenidos += 1
    
    # Esto lo hacemos pensando en un cold start (es decir, ni siquiera van 4 semanas)
    if len(usuarios_activos_iniciales) > 0:
        return retenidos / len(usuarios_activos_iniciales)
    else:
        return 1.0