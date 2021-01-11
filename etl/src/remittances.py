import re
import dateutil.parser as parser
import edn_format

from pymongo import MongoClient

MONGO_HOST = os.environ["MONGO_HOST"]
MONGO_PORT = os.environ["MONGO_PORT"]
MONGO_USER = os.environ["MONGO_INITDB_ROOT_USERNAME"]
MONGO_PASSWORD = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
MONGO_DB = os.environ["MONGO_INITDB_DATABASE"]

client = MongoClient(f'mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/')
db = client[MONGO_DB]
remits = db["remits"]

remit_df = pd.read_csv("/etl/data/remit.csv")

object_fields = ["Callback Confirmation Request Body", "Files", "History", "Payment", "Payment Methods",
                "Payment Response", "Reactions", "Stocks", "Stocks Customer", "Stocks Profit", "Tags"]
date_fields = ["Created At", "Updated At"]


def transform_raw_doc(raw_doc):
    clean_doc = raw_doc
    if "org.bson.types.ObjectId" in raw_doc:
        raw_ids = re.findall("#object[org.bson.types.ObjectId [^\]]+ [^\]]+]", raw_doc)
        for raw_id in raw_ids:
            clean_id = raw_id.replace("#object[", "").replace("]", "").split(" ")[2]
            clean_doc = clean_doc.replace(raw_id, clean_id)
    
    edn_doc = edn_format.loads(clean_doc)
    if type(edn_doc) == edn_format.immutable_list.ImmutableList:
        return [parse_edn(x) for x in edn_doc]
    else:
        return parse_edn(edn_doc)


def parse_value(value):
    if type(value) == tuple and value[0].name == "t/instant":
        return parser.isoparse(value[1])
    else:
        return value


def parse_edn(obj):
    if type(obj) == edn_format.immutable_dict.ImmutableDict:
        parsed_obj = dict()
        for key in obj.keys():
            parsed_obj[key.name.replace(":", "").strip()] = parse_value(obj[key])
        return parsed_obj
    else:
        return obj


def run_etl():
    remits.delete_many({})
    docs = []
    for _, row in remit_df.iterrows():
        doc = dict()
        for column in remit_df.columns:
            if not pd.notna(row[column]):
                doc[column] = None
                continue
                
            if column in object_fields:
                value = transform_raw_doc(row[column])
            elif column in date_fields:
                value = parser.isoparse(row[column])
            else:
                value = row[column]
                
            doc[column.replace(".", "")] = value
        remits.insert_one(doc)

if __name__ == "__main__":
    run_etl()