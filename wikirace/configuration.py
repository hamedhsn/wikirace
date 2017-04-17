
# KAFKA_CLUSTR = ['127.0.0.1:9092'] -- PLEASE FILL
KAFKA_CLUSTR = []


SERIALIZATIO_AVRO = 'avro'
SERIALIZATIO_JSON = 'json'

KFK_PRODUCER = 'Producer'
KFK_CONSUMER = 'Consumer'

# ##### AVRO Schema if Any #####
avro_test_schema = {
}

INGESTION_TOPIC = 'wiki'

# MONGO CONFIG -- PLEASE FILL
MNG_HOST = ""
MNG_PORT = 27000
MNG_USER_NAME = ""
MNG_DBNAME = ""
MNG_PASSWORD = ""
MNG_SSL = False
MNG_SOURCE = None

OUTPUT_COLNM = 'wiki'

DMN = 'https://en.wikipedia.org'
MAX_DEPTH = 10