from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: v.encode("utf-8"),
)

# MUST follow the format: year_gpname_sessiontype
key = "2023_Silverstone_race"

# CSV content as expected by the consumer
value = "driver,position,time\nVerstappen,1,1:29.876\nHamilton,2,1:30.123"

producer.send("f1-historical-data", key=key, value=value)
producer.flush()
print("Message sent.")
