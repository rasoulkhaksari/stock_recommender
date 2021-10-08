from ta.trend import macd_diff, sma_indicator
from ta.momentum import rsi
import pandas as pd
import numpy as np
import yfinance as yf


class Recommender:
    def __init__(self, dbengine, schema) -> None:
        self.engine = dbengine
        self.schema = schema

    def get_tables(self):
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.schema}'"
        df = pd.read_sql(query, self.engine)
        df['Schema'] = self.schema
        return df

    def get_prices(self):
        prices = []
        tbl = self.get_tables()
        for table, schema in zip(tbl.table_name, tbl.Schema):
            prices.append(pd.read_sql(f'SELECT "Date","Close" FROM "{schema}"."{table}"', self.engine))
        return prices

    def max_date(self):
        return pd.read_sql(f'SELECT MAX("Date") FROM "{self.schema}"."{self.get_tables().table_name[0]}"',self.engine)

    def update_DB(self):
        maxdate = self.max_date()['max'][0]
        for stock in self.get_tables().table_name:
            data = yf.download(stock,start=maxdate)
            data = data[data.index>maxdate]
            data = data.reset_index()
            data.to_sql(stock,self.engine,if_exists='append',schema=self.schema)
        print(f'{self.schema} successfully updated')

    def MACD_decision(self,df):
        df['MACD_diff'] = macd_diff(df.Close)
        df['Decision MACD'] = np.where((df.MACD_diff > 0) & (
            df.MACD_diff.shift(1) < 0), True, False)


    def Golden_cross_decision(self,df):
        df['SMA20'] = sma_indicator(df.Close, window=20)
        df['SMA50'] = sma_indicator(df.Close, window=50)
        df['Signal'] = np.where(df['SMA20'] > df['SMA50'], True, False)
        df['Decision GC'] = df.Signal.diff()


    def RSI_SMA_decision(self,df):
        df['RSI'] = rsi(df.Close, window=10)
        df['SMA200'] = sma_indicator(df.Close, window=200)
        df['Decision RSI/SMA'] = np.where((df.Close >
                                        df.SMA200) & (df.RSI < 30), True, False)


    def apply_technicals(self):
        prices = self.get_prices()
        for frame in prices:
            self.MACD_decision(frame)
            self.Golden_cross_decision(frame)
            self.RSI_SMA_decision(frame)
        return prices


    def recommend(self):
        indicators=['Decision MACD','Decision GC','Decision RSI/SMA']
        for symbol,frame in zip(self.get_tables().table_name,self.apply_technicals()):
            if frame.empty is False:
                for indicator in indicators:
                    if frame[indicator].iloc[-1]==True:
                        print(f"{self.schema}: {indicator} Buying Signal for {symbol}")