# GEMINI | Take Home Part 1
# Doug Gorman
# 03.13.23

# OVERVIEW
# 1. Export data from Binance.US API for trading pair BTC-USD in 1 minute candle grain
# 2. Structure (10): Trading Pair, [Open,Close,High,Low] Price, [BTC,USD] Volume, Num Trades, Candle [Open,Close] Time
# 3. Create table and load data to PSQL database

# IMPORT
import json
import requests
import pandas as pd
import time
import psycopg2
from io import StringIO

# VARIABLES | Update as needed
period = 10
interval = '1m'
base_ass = 'btc'  # pair = 'BTCUSD'
quote_ass = 'usd'
pair = f'{base_ass}{quote_ass}'
save_loc = ''
api_name = 'binance'
db_name = ''  # update after creating DB
db_pass = ''  # update after creating DB

# TEMP LISTS
df_temp = []
unix_finish = []

# SET | Establish time interval and return other parameters used
def set_args():

    print('[FUNCTION] set_args')
    print('---------')

    unix_convert = 60 * 60 * 24  # unix daily increments
    unix_now = int(time.time())  # current unix epoch. can also be used to hard code a different epoch
    unix_start = f'{unix_now - (unix_convert * period)}' + '000'
    unix_fin = f'{unix_now}' + '000'
    unix_finish.append(unix_fin)  # add to temp list for use later in script if needed

    print(f'[ARGS] Pair set as [{pair}]')
    print(f'[ARGS] Period set as [{period}] days')
    print(f'[ARGS] Current unix epoch [{unix_now}]')
    print(f'[ARGS] Starting unix epoch [{unix_start}]')
    print(f'[ARGS] Finish unix epoch [{unix_fin}]')
    print('-----')

    get_api(unix_start)


# GET | API status and data
def get_api(unix_start):
    print('[FUNCTION] get_api')
    print('---------')

    pair_up = f'{base_ass}{quote_ass}'.upper()
    api = f'https://api.binance.us/api/v3/klines?symbol={pair_up}&interval={interval}&limit=1000&startTime={unix_start}'
    print(f'[API] {api}')
    request = requests.get(api)
    status = request.status_code
    if status == 200:
        print('[API] Status looks good. Moving on to [structure_data]')
        print('-------')
        data = json.loads(request.text)
        structure_data(data)
    else:
        print('[API STATUS] SOMETHING IS WRONG')
        return


# STRUCTURE | Format API data
def structure_data(data):
    print('[FUNCTION] structure_data')
    print('---------')

    # CREATE | DF from API results
    df = pd.DataFrame(data, columns=[
        'unix_open',
        'price_open',
        'price_high',
        'price_low',
        'price_close',
        f'vol_{base_ass}',
        'unix_close',
        f'vol_{quote_ass}',
        'trade_count',
        'taker_vol_base',
        'taker_vol_quote',
        'ignore'
    ])
    df_len = len(df.index)
    print(f'[DF] DF created with record count [{df_len}]')

    # FIND | First and last unix value to pass to next request
    unix_first = int(df['unix_open'].iloc[0])
    unix_last = int(df['unix_open'].iloc[-1])
    print(f'[DF] Start unix_open value [{unix_first}]')
    print(f'[DF] End unix_open value [{unix_last}]')

    # UPDATE | Add column for readable datetime & pair, remove unnecessary columns, convert DF to string
    df['time_open_candle'] = pd.to_datetime(df['unix_open'], unit='ms', origin='unix')
    df['time_close_candle'] = pd.to_datetime(df['unix_close'], unit='ms', origin='unix')
    df['trade_pair'] = f'{pair}'
    df.drop(columns=['unix_close'], inplace=True)
    df.drop(columns=['taker_vol_base'], inplace=True)
    df.drop(columns=['taker_vol_quote'], inplace=True)
    df.drop(columns=['ignore'], inplace=True)
    df = df.astype(str)

    # APPEND | Set index and append to temp list of DFs
    df = df.set_index('unix_open')
    df_temp.append(df)
    print(f'[DF] Added DF to list [df_temp]')

    # CHECK | If remaining records, get next page. If not, create final DF
    if df_len == 1000 and unix_last < int(unix_finish[0]):
        unix_start = unix_last + 60000  # adds 1 min increment
        print(f'[DF] Moving on to [get_api] with unix_start [{unix_start}]')
        print('-----')
        get_api(unix_start)

    else:
        print(f'[DF] Made it to end as record count [{df_len}] < limit of [1000]')
        print('---')
        print('[DF] Final DF:')
        df_final = pd.concat(df_temp)
        print(df_final)
        print('---')

        # SAVE | CSV file with results
        print('[DF] Creating backup CSV file')
        file_name = f'{api_name}_{pair}_{interval}_{period}d.csv'
        df_final.to_csv(f'{save_loc}{file_name}')
        print(f'[DF] Saved CSV file [{file_name}] to [{save_loc}]')
        print('---')

        print('Moving on to [connect_sql]')
        print('-----')
        create_sql(df_final)


# CREATE | Load structured API data to new table in existing PostgreSQL database
def create_sql(df_final):
    print('[FUNCTION] connect_sql')
    print('---------')

    try:
        # CONNECT | Open connection with existing database
        print(f'[DB] Opening connecting to PSQL database {db_name}')
        conn = psycopg2.connect(database=db_name,
                                host='localhost',
                                user='postgres',
                                password=db_pass,
                                port='5432')
        cur = conn.cursor()
        print('[DB] Connection opened')
        print('---')

        # CREATE | SQL used to create new table if not already exists
        print('[DB] Attempting to create table')
        sql = f"""CREATE TABLE IF NOT EXISTS {api_name}_{pair}_{interval} (
                    unix_open DECIMAL PRIMARY KEY,
                    price_open DECIMAL,
                    price_high DECIMAL,
                    price_low DECIMAL,
                    price_close DECIMAL,
                    vol_{base_ass} DECIMAL,
                    vol_{quote_ass} DECIMAL,
                    trade_count DECIMAL,
                    time_open_candle TIMESTAMP,
                    time_close_candle TIMESTAMP,
                    trade_pair TEXT
                    )"""
        cur.execute(sql)
        conn.commit()
        print(f'[DB] Table created [{db_name}.{api_name}_{pair}_{interval}]')
        print('---')

        # LOAD | Copy data to table
        print(f'[DB] Attempting to copy data to new table')
        sio = StringIO()
        sio.write(df_final.to_csv(index=True, header=False))
        sio.seek(0)

        with cur as c:
            c.copy_from(file=sio, table=f'{api_name}_{pair}_{interval}',
                        columns=['unix_open',
                                 'price_open',
                                 'price_high',
                                 'price_low',
                                 'price_close',
                                 f'vol_{base_ass}',
                                 f'vol_{quote_ass}',
                                 'trade_count',
                                 'time_open_candle',
                                 'time_close_candle',
                                 'trade_pair'],
                        sep=',')
            conn.commit()

        # CLOSE | Disable connection to new table
        print(f'[DB] Data copied to table')
        print('---')
        print(f'[DB] Closing connection and finishing execution')
        print('------------')
        conn.close()

        print('FIN')
        print(':)')

    except Exception as e:
        print(f'[DB] Issue in connection or update: {e}')


if __name__ == "__main__":
    print('---')
    print('Shake and bake')
    print('-----')
    set_args()
