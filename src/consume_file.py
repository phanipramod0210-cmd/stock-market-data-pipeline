#!/usr/bin/env python3
import argparse, json, collections

def consume_file(path):
    counts = collections.Counter()
    prices = {}
    with open(path) as f:
        for line in f:
            msg = json.loads(line.strip())
            counts[msg['symbol']]+=1
            prices.setdefault(msg['symbol'], []).append(msg['price'])
    for s in counts:
        avg = sum(prices[s])/len(prices[s])
        print(f"{s}: count={counts[s]}, avg_price={avg:.2f}")

if __name__ == '__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--file', required=True)
    args=p.parse_args()
    consume_file(args.file)
