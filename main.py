import websocket
import json
import time
import pandas as pd
from binance.client import Client
from sqlalchemy import create_engine
import streamlit as st
from datetime import datetime
import annotated_text as ant 
from annotated_text import annotation

from streamlit_card import card 
import threading
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx


client = Client()
dict_ = client.get_exchange_info()
sym = [i['symbol'] for i in dict_['symbols'] if i['symbol'].endswith('USDT') and 'TRD_GRP_004' in i['permissions'] and i['isMarginTradingAllowed'] == True]
symb =[]
for  i in sym:
    if "UP" not in i and "DOWN" not in i:
        symb.append(i)
symb = [i.lower() + '@kline_1m' for i in symb]
#symb = [i.lower() + '_perpetual@continuousKline_1m' for i in symb]
relevant = '/'.join(symb)
len(symb)
start = time.time()

dfb= pd.DataFrame()
def manipulate(data):
    container = {}
    value_d = data['data']['k']
    price, sym = value_d['c'], value_d['s']
    container['ts'] = data['data']['E']
    container['coin'] = sym
    container['price'] = float(price)
    
    return container

def find_value_with_greatest_difference(lst):
    if len(lst) < 2:
        return lst[0]  # There must be at least two elements in the list
    else:
        last_index_value = lst[-1]
        differences = [(abs(last_index_value - x), x) for x in lst[:-1]]

        max_difference, value = max(differences, key=lambda item: item[0])
        return value
def getreturn(coin):
    global dfb
    result = {}
  
    coin_frame = dfb[dfb.coin == coin]
    coin_frame = coin_frame.sort_values('ts')
    price_list = coin_frame['price'].tolist()
    pivot = find_value_with_greatest_difference(price_list)
    price = price_list[-1]
    #price = coin_frame['price'].iloc[-1]
    change = (price/pivot -1)*100
    result['Coin'] = coin_frame['coin'].iloc[-1]
    #result['Ts'] = coin_frame['ts'].iloc[-1]
    result['Reference Price'] = pivot
    result['Close Price'] = price
    result[f'{period}m % Change'] = change

    if change > 0:
        result['signal'] = 'BUY'
    elif change < 0:
        result['signal'] = 'SELL'
    else :
        result['signal'] = 'NEUTRAL'
    #print(f'{result} --- {time.time()}')
    return result
  

def make_clickable(coin):
    link = f'https://www.binance.com/en/futures/{coin[:-4]}USDT'
    #link = f'https://testnet.binancefuture.com/en/futures/{coin[:-4]}USDT'
    return f'<a target="_blank" href="{link}">{coin}</a>'


def on_message(ws, message):
    my_list = []
    global dfb
    json_message = json.loads(message)
    my_list.append(manipulate(json_message))
    
    new_df = pd.DataFrame(my_list)
    dfb = pd.concat([dfb, new_df], ignore_index=True)
    
def annotate_value(value):
    if value == 'BUY':
        return 'color: green; font-weight: bold'
    elif value == 'SELL':
        return 'color: red; font-weight: bold'
    elif value == 'NEUTRAL':
        return 'color: grey; font-weight: bold'
def count_down():
    time.sleep(10)
    global dfb
    if not dfb.empty:
        st.write(f"Bullish and Bearish Coins")
        
        
         
        with st.empty():
            while True:
                rets = []
                stop = time.time()
                #print(f'{(len(dfb))} -- {(stop-start)/60}')
                #if datetime.now().second  <= 3:
                one_minute_ago = (time.time() - 5*60)*1000
                dfb = dfb[dfb['ts'] > one_minute_ago]
                #print(dfb)
                for coin in dfb.coin.unique():
                    #print(coin)
                    rets.append(getreturn(coin))

                df = pd.DataFrame(rets)
                df_sort = df.iloc[(-df[f'{period}m % Change'].abs()).argsort()]
                df_sort = df_sort.head(10)
                df_sort = df_sort.reset_index(drop=True)
                df_sort['Coin'] = df_sort['Coin'].apply(make_clickable)
                styled_df = df_sort.style.applymap(annotate_value, subset=['signal'])
                #print(df_sort)
                #st.table(styled_df)
                
                st.write(styled_df.to_html(escape = False), unsafe_allow_html = True)
                print(f'{datetime.now()} --- {len(dfb)}')

t= threading.Thread(target=count_down)
t.start()  
ctx = get_script_run_ctx()
add_script_run_ctx(thread=t, ctx=ctx)
     
            
#socket = 'wss://stream.binance.com:9443/stream?streams='+relevant
socket = 'wss://fstream.binance.us/stream?streams='+relevant
#socket = 'wss://stream.binancefuture.com/stream?streams='+relevant
ws = websocket.WebSocketApp(socket,on_message=on_message)
ws.run_forever()

#print('Connect')
