from kafka import KafkaProducer
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: str(v).encode('utf-8'))

impression_id_pool = [f"imp_{i}" for i in range(1000)]  
start_time = int(time.time() * 1000)

print("Starting to send impressions...")
for i in range(100):  
    imp_id = random.choice(impression_id_pool)
    user_id = f"user_{random.randint(1,100)}"
    ad_id = f"ad_{random.randint(1,20)}"
    timestamp = start_time + i * 100  
    msg = f"{imp_id},{user_id},{ad_id},{timestamp}"
    producer.send('impressions', msg)
    print(f"Sent impression: {msg}")
    
    if random.random() < 0.3:
        time.sleep(random.uniform(0.2, 2))
        click_id = f"click_{i}"
        click_ts = timestamp + random.randint(200, 5000)
        click_msg = f"{click_id},{imp_id},{click_ts}"
        producer.send('clicks', click_msg)
        print(f"Sent click: {click_msg}")
    time.sleep(0.05)  

producer.flush()
print("Done sending messages.")