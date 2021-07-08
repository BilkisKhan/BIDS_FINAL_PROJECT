import pandas as pd
import numpy as np
import urllib.request as urlreq
import pymysql
import yaml
from datetime import timedelta, datetime as dt
import time
import json
import logging
from collections import OrderedDict
import os
import sys
import traceback


# YAML file containing database login credentials
cred_file = "./creds_cron.yaml"

transactionType = ''

if not os.path.exists(cred_file):
    print("creds.yaml not found: did you rename creds_template.yaml?")
    sys.exit()


def init_logger():
    logging.basicConfig(
        level="INFO", format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger()
    return logger


#################################
##### SQL Utility Functions #####
#################################


def db_connect(host, user, password, database):
    conn = pymysql.connect(host=host, user=user,
                           password=password, db=database)
    print(conn)
    cursor = conn.cursor()
    return conn, cursor


def fetchone(sql_cursor, query):
    sql_cursor.execute(query)
    answer = None
    tmp = sql_cursor.fetchone()
    if tmp is not None:
        answer = tmp[0]
    return answer


def npdt2str(a_dt):
    # Convert numpy datetime64 to a string compatible with MySQL datetime
    return pd.to_datetime(str(a_dt)).strftime("%Y-%m-%d %H:%M:%S")


###########################
##### Ticker #####
###########################


def get_tickers(inc_stocks=True, inc_bonds=True, inc_commods=True, inc_fx=True):
    stocks = ['BF-B', 'MCD', 'XLP']
    bonds = []
    commods = []
    fx = []
    tickers = inc_stocks * stocks + inc_bonds * \
        bonds + inc_commods * commods + inc_fx * fx
    tickers = {i: "AlphaVantage" for i in sorted(tickers)}
    return tickers


#########################################
##### Data Source #####
#########################################


def get_data_alphavantage(logger, ticker, api_key, outputsize="full", intraday=False, interval='5min'):

    logger("... getting data from AlphaVantage: {}".format(ticker))

    base_url = "https://www.alphavantage.co/query?function="
    print(f'transactionType>>>>>>>>>>' + transactionType)
    if transactionType == 'intraday':
        base_url = "{}TIME_SERIES_DAILY_ADJUSTED&".format(base_url)
        option_url = "symbol={}&interval={}&outputsize={}&apikey={}".format(
            ticker, interval, outputsize, api_key)
    else:
        # base_url = "{}TIME_SERIES_DAILY_ADJUSTED&".format(base_url)
        base_url = "{}TIME_SERIES_MONTHLY_ADJUSTED&".format(base_url)

        option_url = "symbol={}&outputsize={}&apikey={}".format(
            ticker, outputsize, api_key)

    data = json.loads(urlreq.urlopen(base_url + option_url).read().decode())

    ts_data = data[[i for i in data.keys() if 'Time Series' in i][0]]

    ts = pd.DataFrame(data=list(ts_data.values()), index=list(ts_data.keys()))
    ts.columns = [i.split(".")[-1].strip() for i in ts.columns]
    ts.index = pd.to_datetime(ts.index)
    ts.sort_index(inplace=True)
    ts.sort_index(inplace=True)

    # if intraday:
    if transactionType:
        print("Inside Intrday when intraday ")
        # ts['adjusted close'] = None
        # ts['dividend amount'] = None
        ts['split coefficient'] = None
    columns = ["open", "high", "low", "close", "adjusted close",
               "volume", "dividend amount", "split coefficient"]
    ts = ts[columns]
    ts = ts['2008-01-18':'2021-09-18']

    return ts


#####################################
##### Database Update  #####
#####################################


def insert_ticker(
        logger,
        sql_conn,
        sql_cursor,
        data_source_info,
        wait_seconds=2,
        cutoff_hour=17,
        tickers=[]):

    start = time.time()
    ticker_list = list(tickers.keys())
    dtnow = dt.now()
    if dtnow.hour < cutoff_hour or dtnow.weekday() > 4:
        update_through_date = dtnow - pd.tseries.offsets.BDay(n=1)
        print(f"Inside dtnow.hour >>>>>>>>>>>>>>>")
        print(update_through_date)
    else:
        update_through_date = dt(
            dtnow.year, dtnow.month, dtnow.day, hour=23, minute=59)
        print(f"Inside else  >>>>>>>>>>>>>>>")
        print(update_through_date)

    while ticker_list:
        ticker = ticker_list.pop(0)
        data_source_name = tickers[ticker]
        logger("Processing {} ({} remaining)".format(ticker, len(ticker_list)))

        try:

            last_dt_in_db = fetchone(
                sql_cursor,
                query="SELECT MAX(SampleTime) FROM DataSourcePriceObservation WHERE Security='{}'".format(ticker))
            # query="SELECT GetLastSampleTimeForSecurity('{}')".format(ticker))

            print("last date in db >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            print(last_dt_in_db)

            if (last_dt_in_db is not None) and (last_dt_in_db.date() == update_through_date.date()):
                logger("... no update required: record is up to date.")
                continue

            if last_dt_in_db is None:
                # Happens if new symbol is added but no data exists.
                logger("... seeding new data through {}".format(
                    update_through_date.date()))
                seed_mode = True
            else:
                seed_mode = False

            price_update(
                logger=logger,
                sql_conn=sql_conn,
                sql_cursor=sql_cursor,
                ticker=ticker,
                data_source_name=data_source_name,
                # data_source_id=data_source_id,
                data_source_info=data_source_info,
                last_dt_in_db=last_dt_in_db,
                update_through_date=update_through_date,
                seed_mode=seed_mode)

            if wait_seconds > 0 and len(ticker_list) > 0:
                logger("... waiting {} seconds so we don't spam the data provider's server.".format(
                    wait_seconds))
                time.sleep(wait_seconds)

        except:
            logger("... FAILED!")
            ticker_list.append(ticker)
            traceback.print_exc()
            logger("... waiting {} seconds so we don't spam the data provider's server.".format(
                wait_seconds))
            time.sleep(wait_seconds)

    end = time.time()
    logger("Processing took {:.2f} minutes".format((end - start) / 60.0))
    logger("***********Cron job completed, *****************")


def price_update(
        logger,
        sql_conn,
        sql_cursor,
        ticker,
        data_source_name,
        # data_source_id,
        data_source_info,
        last_dt_in_db,
        update_through_date,
        seed_mode=False):

    if data_source_name == "AlphaVantage":
        http_call = {True: "full", False: "compact"}
        raw_data = get_data_alphavantage(
            logger=logger,
            ticker=ticker,
            api_key=data_source_info[data_source_name]['api_key'],
            outputsize=http_call[seed_mode],
            # intraday="intraday" in str(sql_conn.db).lower())
            intraday="intraday" in transactionType)
        # isolate the data to update
        if last_dt_in_db is not None:
            raw_data = raw_data[raw_data.index > last_dt_in_db]
        raw_data = raw_data[:update_through_date]  # trim
    else:
        logger("Datasource not supported: {}".format(data_source_name))

    # replace NaN with None for SQL compatibility
    raw_data = raw_data.where(raw_data.notnull(), None)

    # raw_data["data_source_id"] = str(data_source_id)
    raw_data["ticker"] = ticker

    if not raw_data.empty:
        logger('... inserting into table: DataSourcePriceObservation')
        stmt = """
            INSERT INTO
            DataSourcePriceObservation (
                SampleTime,
                OpenPrice,
                HighPrice,
                LowPrice,
                ClosePrice,
                AdjustedClosePrice,
                Volume,
                DividendAmount,
                SplitCoefficient,
                Security)
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,              
                %s);
            """
        tuple_data = [tuple(x) for x in raw_data.to_records(index=True)]

        # convert numpy datetime64 to a date string that MySQL can handle
        for i in range(len(tuple_data)):
            tuple_data[i] = tuple((npdt2str(x) if type(
                x) == np.datetime64 else x for x in tuple_data[i]))

        sql_cursor.executemany(stmt, tuple_data)
        sql_conn.commit()


##########################################
##### Database Maintenance #####
##########################################


def get_creds(cred_file, database_name):
    with open(cred_file, "r") as f:
        data = yaml.load(f, Loader=yaml.FullLoader)

    # database creds
    db_info = {}
    db_info['user'] = data['databases'][database_name]['user']
    db_info['password'] = data['databases'][database_name]['password']
    db_info['host'] = data['databases'][database_name]['host']

    # datasource creds
    data_source_info = {}
    for datasource in data['datasources']:
        data_source_info[datasource] = data['datasources'][datasource]

    return db_info, data_source_info


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-intraday", "--intraday",
                        action="store_true", default=False)
    parser.add_argument("-daily", "--daily",
                        action="store_true", default=False)
    parser.add_argument("-monthly", "--monthly",
                        action="store_true", default=False)

    args = parser.parse_args()

    logger = init_logger().info

    for dbname, truefalse in args._get_kwargs():
        if truefalse:
            # database_name = "PRICES_{}".format(dbname.upper())
            database_name = "PRICES_INTRADAY"
            print(dbname)
            transactionType = dbname

            logger("USING DATABASE: {}".format(database_name))

            db_info, data_source_info = get_creds(
                cred_file, database_name=database_name)

            sql_conn, sql_cursor = db_connect(
                host=db_info['host'],
                user=db_info['user'],
                password=db_info['password'],
                database=database_name)

            tickers = get_tickers()
            print(tickers)

            insert_ticker(
                logger=logger,
                sql_conn=sql_conn,
                sql_cursor=sql_cursor,
                data_source_info=data_source_info,
                wait_seconds=5,
                tickers=tickers)
            sql_conn.close()
