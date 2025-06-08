from fastapi import FastAPI, WebSocket

import asyncio, json, uvicorn

from aggregator import Aggregator, start_tasks   # 复用你的聚合逻辑


app = FastAPI()

agg = Aggregator(tick=0.01)

asyncio.create_task(start_tasks(agg))            # 后台永续聚合


@app.get("/symbols")

def list_symbols():

    return ["BTC/USDT", "ETH/USDT"]              # 会议改成你的 SYMBOLS
from fastapi import FastAPI, WebSocket

import asyncio, json, uvicorn

from aggregator import Aggregator, start_tasks   # 复用你的聚合逻辑


app = FastAPI()

agg = Aggregator(tick=0.01)

asyncio.create_task(start_tasks(agg))            # 后台永续聚合


@app.get("/symbols")

def list_symbols():

    return ["BTC/USDT", "ETH/USDT"]              # 会议改成你的 SYMBOLS


@app.websocket("/ws/{symbol}")

async def orderbook(ws: WebSocket, symbol: str):

    await ws.accept()

    while True:

        await ws.send_text(json.dumps(agg.get(symbol.upper())))

        await asyncio.sleep(1)                   # 推送频率 1s


if __name__ == "__main__":

    uvicorn.run("backend:app", host="127.0.0.1", port=8000, reload=False)


