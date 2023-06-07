import sys
from kafka import KafkaProducer
from json import dumps
import time
import json
import pandas as pd
import pyupbit
import boto3
from datetime import datetime
import numpy as np
from numpy import nan
import os

def producer():
    ticker = 'KRW-BTC'
    interval = 'minute1'
    to = datetime.now().strftime('%Y-%m-%d %H:%M')
    count = 60
    df=pyupbit.get_ohlcv(ticker=ticker,interval=interval,to=to,count=count)
    df=df.reset_index()
    df=df.rename(columns = {'index':'collect_time'})
    df.collect_time=df.collect_time.astype(str)

    server = "3.38.255.103:9092"
    topic_name = "miniproject_upbit_final"

    pr = KafkaProducer(
                        bootstrap_servers = [server]
                        , api_version=(0, 10, 2)
                    )

    print("Start Insert data to Kafka")
    for i in range(len(df)):
        m1 = {}
        m1[i] = df.loc[i].to_dict()
        m2 = json.dumps(m1).encode("utf-8")
        pr.send(topic_name, m2)
    pr.flush()

