#!/usr/bin/env python3
import argparse, json, time, random, os

def produce_to_file(path, count=100):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        for _ in range(count):
            msg = {'symbol': random.choice(['AAPL','GOOG','MSFT','AMZN']),'price': round(random.uniform(100, 2000),2),'timestamp': time.time()}
            f.write(json.dumps(msg)+'\n')
    print('Wrote', count, 'messages to', path)

if __name__ == '__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--bootstrap', default='localhost:9092')
    p.add_argument('--topic', default='stock_topic')
    p.add_argument('--mockfile', default=None)
    p.add_argument('--count', type=int, default=100)
    args=p.parse_args()
    if args.mockfile:
        produce_to_file(args.mockfile, args.count)
    else:
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(bootstrap_servers=[args.bootstrap], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            for _ in range(args.count):
                msg = {'symbol': random.choice(['AAPL','GOOG','MSFT','AMZN']),'price': round(random.uniform(100, 2000),2),'timestamp': time.time()}
                producer.send(args.topic, msg)
                print('Sent', msg)
                time.sleep(1)
        except Exception as e:
            print('Kafka not available locally:', e)
