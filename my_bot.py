import settings
import asyncio
import gc
import numpy as np
import pandas as pd
from rich import print
import time
import talib
import pybotters

apis = {
    "bybit": [settings.bybit_key, settings.bybit_secret],
    "bybit_testnet": [settings.bybit_testnet_key, settings.bybit_testnet_secret],
}

RestAPI_url = {
    "bybit": "https://api.bybit.com",
    "bybit_testnet": "https://api-testnet.bybit.com",
}

wss_url = {
    "bybit": "wss://stream.bybit.com/realtime",
    "bybit_testnet": "wss://stream-testnet.bybit.com/realtime",
}

pips = 0.5
symbol = "BTCUSD"
buy_entry, sell_entry, retouch_20 = False, False, False


def calc_indicators(df):
    # インジケーター
    df["9EMA"] = talib.EMA(df["close"], timeperiod=9)
    df["20EMA"] = talib.EMA(df["close"], timeperiod=20)
    df["45EMA"] = talib.EMA(df["close"], timeperiod=45)
    df["100EMA"] = talib.EMA(df["close"], timeperiod=100)
    df["ATR"] = talib.ATR(df["high"], df["low"], df["close"], timeperiod=15)
    # エントリー指値価格
    df["buy_price"] = df["close"] + pips
    df["sell_price"] = df["close"] - pips
    # 決済指値を計算
    limit_price_dist = df["ATR"] * 1.5
    limit_price_dist = np.maximum(2, (limit_price_dist / pips).round().fillna(2)) * pips
    df["buy_limit"] = df["close"] - limit_price_dist
    df["sell_limit"] = df["close"] + limit_price_dist

    return df


async def main():
    async with pybotters.Client(
        apis=apis, base_url=RestAPI_url["bybit_testnet"]
    ) as client:

        # データストアのインスタンスを生成する
        store = pybotters.BybitDataStore()

        # REST API由来のデータ(オーダー・ポジション・残高)を初期データとしてデータストアに挿入する
        resps = await asyncio.gather(
            client.get(
                "/v2/public/kline/list",
                params={
                    "symbol": symbol,
                    "interval": 1,
                    "from": int(time.time()) - 12000,
                    "limit": 200,
                },
            ),
        )
        ohlcv = await asyncio.gather(*[r.json() for r in resps])

        l_time = [d.get("open_time") for d in ohlcv[0]["result"]]
        l_open = [d.get("open") for d in ohlcv[0]["result"]]
        l_high = [d.get("high") for d in ohlcv[0]["result"]]
        l_low = [d.get("low") for d in ohlcv[0]["result"]]
        l_close = [d.get("close") for d in ohlcv[0]["result"]]

        df = pd.DataFrame(
            dict(
                open_time=pd.Series(l_time, dtype=int),
                open=pd.Series(l_open, dtype=float),
                high=pd.Series(l_high, dtype=float),
                low=pd.Series(l_low, dtype=float),
                close=pd.Series(l_close, dtype=float),
            )
        )
        df = calc_indicators(df)

        # df["open_time"] = pd.to_datetime(df["open_time"], unit="s")
        print(df)

        # WebSocket接続
        wstask = await client.ws_connect(
            wss_url["bybit_testnet"],
            send_json={
                "op": "subscribe",
                "args": ["klineV2.1.BTCUSD", "position", "order"],
            },
            hdlr_json=store.onmessage,
        )

        # WebSocketでデータを受信するまで待機
        while not all([len(store.kline)]):
            await store.wait()

        # メインループ
        while True:
            # データ参照
            new_1m_data = dict(kline=store.kline.find())
            position = store.position_inverse.find()
            order = store.order.find()

            # 未登録の1分足データがあれば処理を行う
            if (
                len(new_1m_data["kline"]) > 1
                and new_1m_data["kline"][1]["start"] != df.iloc[-1]["open_time"]
            ):
                # 確定した1分足データを更新
                df.iloc[-1, 0] = new_1m_data["kline"][0]["start"]
                df.iloc[-1, 1] = new_1m_data["kline"][0]["open"]
                df.iloc[-1, 2] = new_1m_data["kline"][0]["high"]
                df.iloc[-1, 3] = new_1m_data["kline"][0]["low"]
                df.iloc[-1, 4] = new_1m_data["kline"][0]["close"]
                # 新しい1分足データを追加
                df = df.append(
                    {
                        "open_time": new_1m_data["kline"][1]["start"],
                        "open": new_1m_data["kline"][1]["open"],
                        "high": new_1m_data["kline"][1]["high"],
                        "low": new_1m_data["kline"][1]["low"],
                        "close": new_1m_data["kline"][1]["close"],
                    },
                    ignore_index=True,
                )
                df["open_time"] = df["open_time"].astype(int)
                df = calc_indicators(df)
                # 確定した1分足データ
                df_1bf = df.iloc[-2]
                df_2bf = df.iloc[-3]

                # 買い方向のパーフェクトオーダー
                if (
                    df_1bf["9EMA"] > df_1bf["20EMA"]
                    and df_1bf["20EMA"] > df_1bf["100EMA"]
                ):
                    # 未執行の注文があればキャンセル
                    if buy_entry and len(order) > 0:
                        await client.post(
                            "/v2/private/order/cancelAll",
                            data={
                                "symbol": symbol,
                            },
                        )
                    # 未決済ポジションがあれば決済指値をトレール
                    if len(position) > 0 and position[0]["side"] == "Buy":
                        await client.post(
                            "/v2/private/position/trading-stop",
                            data={
                                "symbol": symbol,
                                "stop_loss": df_1bf["buy_limit"],
                            },
                        )
                    else:
                        # 20EMA再タッチ判定
                        if buy_entry and not retouch_20:
                            if (
                                df_1bf["low"] <= df_1bf["20EMA"]
                                and df_1bf["20EMA"] < df_1bf["close"]
                            ) or (
                                df_2bf["close"] <= df_1bf["20EMA"]
                                and df_1bf["20EMA"] < df_1bf["close"]
                            ):
                                retouch_20 = True
                        # パーフェクトオーダー後初エントリーまたは20EMA再タッチからエントリー
                        if not buy_entry or retouch_20:
                            buy_entry = True
                            retouch_20 = False
                            # 新規買い指値注文
                            await client.post(
                                "/v2/private/order/create",
                                data={
                                    "symbol": symbol,
                                    "side": "Buy",
                                    "order_type": "Limit",
                                    "qty": 10,
                                    "price": df_1bf["buy_price"],
                                    "stop_loss": df_1bf["buy_limit"],
                                    "time_in_force": "GoodTillCancel",
                                },
                            )
                else:
                    buy_entry, retouch_20 = False, False
                    # 未執行の買い指値注文があればキャンセル
                    if len(order) > 0 and order[0]["side"] == "Buy":
                        await client.post(
                            "/v2/private/order/cancel",
                            data={
                                "symbol": symbol,
                                "order_id": order[0]["order_id"],
                            },
                        )

                # 売り方向のパーフェクトオーダー
                if (
                    df_1bf["9EMA"] < df_1bf["20EMA"]
                    and df_1bf["20EMA"] < df_1bf["100EMA"]
                ):
                    # 未執行の注文があればキャンセル
                    if sell_entry and len(order) > 0:
                        await client.post(
                            "/v2/private/order/cancelAll",
                            data={
                                "symbol": symbol,
                            },
                        )
                    # 未決済ポジションがあれば決済指値をトレール
                    if len(position) > 0 and position[0]["side"] == "Sell":
                        await client.post(
                            "/v2/private/position/trading-stop",
                            data={
                                "symbol": symbol,
                                "stop_loss": df_1bf["sell_limit"],
                            },
                        )
                    else:
                        # 20EMA再タッチ判定
                        if sell_entry and not retouch_20:
                            if (
                                df_1bf["high"] >= df_1bf["20EMA"]
                                and df_1bf["20EMA"] > df_1bf["close"]
                            ) or (
                                df_2bf["close"] >= df_1bf["20EMA"]
                                and df_1bf["20EMA"] > df_1bf["close"]
                            ):
                                retouch_20 = True
                        # パーフェクトオーダー後初エントリーまたは20EMA再タッチからエントリー
                        if not sell_entry or retouch_20:
                            sell_entry = True
                            retouch_20 = False
                            # 新規売り指値注文
                            await client.post(
                                "/v2/private/order/create",
                                data={
                                    "symbol": symbol,
                                    "side": "Sell",
                                    "order_type": "Limit",
                                    "qty": 10,
                                    "price": df_1bf["sell_price"],
                                    "stop_loss": df_1bf["sell_limit"],
                                    "time_in_force": "GoodTillCancel",
                                },
                            )
                else:
                    sell_entry, retouch_20 = False, False
                    # 未執行の売り指値注文があればキャンセル
                    if len(order) > 0 and order[0]["side"] == "Sell":
                        await client.post(
                            "/v2/private/order/cancel",
                            data={
                                "symbol": symbol,
                                "order_id": order[0]["order_id"],
                            },
                        )

                # 確定済みの1分足データはDataStoreからクリアする
                store.kline._clear()
                # 500件以上の場合、最新400件にリサイズする
                if len(df) >= 250:
                    df = df[-200:]
                    gc.collect()
                print(df)

            # 次の値動きイベントまで待機
            await store.kline.wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
