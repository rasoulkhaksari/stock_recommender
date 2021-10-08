import time
import sqlalchemy
import yaml
import pandas as pd
import yfinance as yf
import logging
from recommender import Recommender

def get_config():
    with open("config.yml", "r") as stream:
        conf = yaml.load(stream, Loader=yaml.FullLoader)
        conf['dbengine'] = sqlalchemy.create_engine(conf['DB_CONNECTION'])
        return conf

def get_stocks(market):
    if market=='Nifty50':
        nifty = pd.read_html('https://en.wikipedia.org/wiki/NIFTY_50')[1]
        nifty = nifty.Symbol.to_list()
        return [i+'.NS' for i in nifty]
    elif market=='RTSI':
        rts = pd.read_html('https://en.wikipedia.org/wiki/RTS_Index')[1]
        rts = rts['Ticker symbol'].to_list()
        return [i+'.ME' for i in rts]
    else:
        bovespa = pd.read_html('https://en.wikipedia.org/wiki/List_of_companies_listed_on_B3')[0]
        bovespa = bovespa.Ticker.to_list()
        return [i+'.SA' for i in bovespa]


def initialize_db(config,indices):
    try:
        count=config['dbengine'].execute(f"select count(*) from pg_database where datname = '{config['DB_NAME']}'").scalar()
        if count==0:
            conn = config['dbengine'].connect()
            conn.execute('commit')
            conn.execute(f"CREATE DATABASE {config['DB_NAME']} ")
            conn.close()
        config['dbengine'] = sqlalchemy.create_engine(f"{config['DB_CONNECTION']}{config['DB_NAME']}")
        for schema in indices:
            count = config['dbengine'].execute(f"select  count(*) from information_schema.schemata  where catalog_name = '{config['DB_NAME']}' and schema_name ='{schema}'").scalar()
            if count==0:
                config['dbengine'].execute(sqlalchemy.schema.CreateSchema(schema))
    except:
        logging.error('Database can not be initialized')

def download_store_stock_data(config,indices):
    for schema in indices:
        count=config['dbengine'].execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{schema}'").scalar()
        if count==0:
            stocks=get_stocks(schema)
            for stock in stocks[:1]:
                df = yf.download(stock, start=config['STOCK_DATA_START'])
                df = df.reset_index()
                df.to_sql(name=stock, con=config['dbengine'], schema=schema)


def get_recommendation(recommenders):
    try:
        while True:
            for recommender in recommenders:
                recommender.update_DB()
                recommender.recommend()
            time.sleep(300)
    except Exception as e:
        time.sleep(300)
        get_recommendation(recommenders)

if __name__ == '__main__':
    logging.basicConfig()
    config = get_config()
    indices=['Nifty50', 'RTSI', 'Bovespa']
    initialize_db(config,indices)
    download_store_stock_data(config,indices)
    nifty_recommender = Recommender(config['dbengine'],'Nifty50')
    rsti_recommender = Recommender(config['dbengine'],'RTSI')
    bovespa_recommender = Recommender(config['dbengine'],'Bovespa')
    get_recommendation([nifty_recommender,rsti_recommender,bovespa_recommender])
