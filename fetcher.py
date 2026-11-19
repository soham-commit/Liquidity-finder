import asyncio
import websockets
import orjson
import time
from collections import deque
import shutil
from rich.layout import Layout
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
liq_window = deque()
data_of_60_sec = 0.0
short_data_of_60_sec = 0.0
long_data_of_60_sec = 0.0
cvd = {"BIN_FUT": 0.0, "BIN_SPOT": 0.0, "BYBIT": 0.0, "OKX": 0.0}
w1_trades = "wss://fstream.binance.com/market/ws/ethusdt@aggTrade"
w1_liqs = "wss://fstream.binance.com/public/ws/ethusdt@forceOrder"
w2 = "wss://stream.binance.com:9443/ws/ethusdt@aggTrade"
w3 = "wss://stream.bybit.com/v5/public/linear"
w4 = "wss://ws.okx.com:8443/ws/v5/public"
w3_payload = {"op":"subscribe", "args" : ["publicTrade.ETHUSDT", "allLiquidation.ETHUSDT"]}
w4_payload = {"op": "subscribe", "args": [{"channel": "trades", "instId": "ETH-USDT-SWAP"}, {"channel": "liquidation-orders", "instType": "SWAP"}]}
def process_trade(exchange, qty, pr, is_sell, whale_limit):
    if is_sell:
        cvd[exchange] -= qty
    else:
        cvd[exchange] += qty
    if qty >= whale_limit:
        side_message = "SELL" if is_sell else "BUY"
        print(f"[{exchange}] : {cvd[exchange]:.2f} | Whale : {side_message} | CURRENT ETH PRICE : {pr} | qty : {qty}")
def process_liquidation(exchange, pr_liq, qty_liq, side):
    now = time.time()
    usd = pr_liq*qty_liq
    liq_window.append((now, usd, side))
    if side == "SELL":
        print(f"[{exchange}] A Long Pos. just liq. at {pr_liq} with qty : {qty_liq}")
    if side == "BUY":
        print(f"[{exchange}] A Short Pos. just liq, at {pr_liq} with qty : {qty_liq}")
    while liq_window and liq_window[0][0] < now - 60:
        liq_window.popleft()
    short_data_of_60_sec = sum(item[1] for item in liq_window if item[2] == "BUY")
    long_data_of_60_sec = sum(item[1] for item in liq_window if item[2] == "SELL")
    if (short_data_of_60_sec + long_data_of_60_sec) >=300_000:
        width = shutil.get_terminal_size((80,20)).columns
        print(f"\033[1;33m{'='*width} WARNING: SHORT LIQ: {short_data_of_60_sec} and LONG LIQ: {long_data_of_60_sec} {'='*width}\033[0m")
async def w1_trade_fetch():
    while True:
        try:
            async with websockets.connect(w1_trades, ping_interval=20, ping_timeout=10) as ws:
                print("Connected to Binance(x1) Trade")
                async for raw in ws:
                    msg = orjson.loads(raw)
                    qty = float(msg["q"])
                    pr = float(msg["p"])
                    isbuymaker = msg["m"]
                    process_trade("BIN_FUT", qty, pr, isbuymaker, 50)
        except Exception as e:
            print(f"ERROR OCCURED IN TRADE SYSTEM WITH CODE: {e}")
            await asyncio.sleep(3)
async def w1_liq_fetch():
    while True:
        try:
            async with websockets.connect(w1_liqs, ping_interval=20, ping_timeout=10) as ws:
                print("Connected to Binance(x1) LIQUID")
                async for raw in ws:
                    msg = orjson.loads(raw)
                    side = msg["o"]
                    qty_liq = float(side["q"])
                    pr_liq = float(side["ap"])
                    process_liquidation("BIN_FUT",  pr_liq, qty_liq, side["S"])
        except Exception as e:
            print(f"ERROR IN w1_liq_fetch: {e}")
            await asyncio.sleep(3)
async def liq_data_every_60sec():
    global short_data_of_60_sec,long_data_of_60_sec
    while True:
        await asyncio.sleep(60)
        now = time.time()
        while liq_window and liq_window[0][0] < now - 60:
            liq_window.popleft()
        short_data_of_60_sec = sum(item[1] for item in liq_window if item[2] == "BUY")
        long_data_of_60_sec = sum(item[1] for item in liq_window if item[2] == "SELL")
        print(f"Total liq in Short(forced to buy): {short_data_of_60_sec:.2f} | Total liq. in Long(forced to sell): {long_data_of_60_sec:.2f}, BIN_FUT CVD : {cvd['BIN_FUT']:.2f} \n BIN_SPOT CVD: {cvd['BIN_SPOT']:.2f} | BYBIT CVD : {cvd['BYBIT']:.2f} | OKX CVD: {cvd['OKX']:.2f}")
        
async def w2_fetch():
    while True:
        try:
            async with websockets.connect(w2, ping_interval=20, ping_timeout=10) as ws:
                print("Connected To Binancex1 SPOT")
                async for raw in ws:
                    msg = orjson.loads(raw)
                    qty = float(msg["q"])
                    pr = float(msg["p"])
                    isbuymaker = msg["m"]
                    process_trade("BIN_SPOT", qty, pr, isbuymaker, 25 )
        except Exception as e:
            print(f"Error Found as {e} in w2_fetch ")
            await asyncio.sleep(3)
async def w3_fetch():
    while True:
        try:
            async with websockets.connect(w3, ping_interval=20, ping_timeout=10) as ws:
                print("Connected to BYBITx1 ")
                await ws.send(orjson.dumps(w3_payload).decode())
                payload = {"op" : "ping"}
                async def ping_w3():
                    while True:
                        await ws.send(orjson.dumps(payload).decode())
                        await asyncio.sleep(19)
                ping = asyncio.create_task(ping_w3())
                try: 
                    async for raw in ws:
                        msg = orjson.loads(raw)
                        if msg and "topic" not in msg:
                            continue                
                        tp = msg.get("topic")
                        if msg.get("topic") == "publicTrade.ETHUSDT":
                            for item in msg["data"]:
                                qty = float(item["v"])
                                pr = float(item["p"])
                                is_sell = (item["S"] == "Sell")
                                process_trade("BYBIT", qty, pr, is_sell, 50)
                        if msg.get("topic") == "allLiquidation.ETHUSDT":
                            for item in msg["data"]:
                                qty_liq = float(item["v"])
                                pr_liq = float(item["p"])
                                side = "SELL" if item["S"] == "buy" else "BUY"
                                process_liquidation("BYBIT", pr_liq, qty_liq, side)
                finally:
                    ping.cancel()
                    try:
                        await ping
                    except asyncio.CancelledError:
                        pass
        except Exception as e:
            print(f"Error find in w3_fetch as : {e}")
            await asyncio.sleep(3)
async def w4_fetch():
    while True:
        try:
            async with websockets.connect(w4, ping_interval=20, ping_timeout=10) as ws:
                print("Connected to OKX")
                await ws.send(orjson.dumps(w4_payload).decode())
                async def ping_w4():
                    while True:
                        
                        await ws.send("ping")
                        await asyncio.sleep(19)
                ping = asyncio.create_task(ping_w4())
                try:
                    async for raw in ws:
                        if raw == "pong":
                            continue
                        msg=orjson.loads(raw)
                        if "data" not in msg:
                            continue
                        channel = msg["arg"]["channel"]
                        if channel == "trades":
                            for item in msg["data"]:
                                qty = float(item["sz"]) * 0.1
                                pr = float(item["px"])
                                is_sell = (item["side"] == "sell")
                                process_trade("OKX", qty, pr, is_sell, 50)
                        if channel == "liquidation-orders":
                            for block in msg["data"]:
                                if block.get("instId") != "ETH-USDT-SWAP":
                                    continue
                                for item in block["details"]:
                                    qty_eth = float(item["sz"]) * 0.1
                                    pr_liq = float(item["bkPx"])
                                    side = item["side"].upper()
                                    process_liquidation("OKX", pr_liq, qty_eth, side)
                finally:
                    ping.cancel()
                    try:
                        await ping
                    except asyncio.CancelledError:
                        pass
        except Exception as e:
            print(f"Error occured in w4_fetch as : {e}")
            await asyncio.sleep(3)
async def main():
    await asyncio.gather(w1_trade_fetch(),
                         w1_liq_fetch(),
                         w2_fetch(),
                         w3_fetch(),
                         w4_fetch(), 
                         liq_data_every_60sec()
                         )
    
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n Closing The Program!")

