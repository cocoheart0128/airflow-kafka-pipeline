from kafka import KafkaConsumer
from json import loads #읽어오는 함수
import time
import datetime
import mysql.connector
import pandas as pd
from datetime import datetime
import time

def mysql_insert():

    host='testdb.cjibwrwuxobb.ap-northeast-2.rds.amazonaws.com'
    port='3306'
    database='testdb'
    user='kexin'
    password='megazone2023!'
    table='upbit'
    conn = mysql.connector.connect(host=host, port=port, database=database, user=user, password=password)
    cur = conn.cursor()

    # kafka consumer 연결
    Server = '3.38.255.103:9092'
    topic_name = "miniproject_upbit_final"
    consumer=KafkaConsumer(topic_name, #읽어올 토픽의 이름 필요.
                            bootstrap_servers=[Server], # 어떤 서버에서 읽어 올지 지정
                            auto_offset_reset="earliest", # 어디서부터 값을 읽어올지 (earliest 가장 처음 latest는 가장 최근)
                            enable_auto_commit=True, # 완료되었을 떄 문자 전송
                            consumer_timeout_ms=1000 # 입력한 ms초  이후에 메시지가 오지 않으면 없는 것으로 취급.
                        )
    print("Start Insert data to MYSQL")

    for mes in consumer:
        # time.sleep(3)
        data = list(pd.DataFrame(eval(mes.value.decode('utf-8')).values()).loc[0])
        value=(data[0],data[1],data[2],data[3],data[4],data[5],data[6])
        query=f"insert into testdb.upbit values {value}"
        cur.execute(query)
        result = cur.fetchall()
    #     print(result)
    conn.commit()
    conn.close()
