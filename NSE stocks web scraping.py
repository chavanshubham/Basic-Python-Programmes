from pandas.core.indexes.base import Index
import scrapy
import pandas as pd
import json
import os
from scrapy.downloadermiddlewares.retry import get_retry_request
from scrapy.crawler import CrawlerProcess



class NSESpider(scrapy.Spider):

    name = 'nse'
    custom_settings = { 
        'DOWNLOAD_DELAY': 2.5,
        'BOT_NAME': 'nse_data',
        'SPIDER_MODULES': ['nse_data.spiders'],
        'NEWSPIDER_MODULE': 'nse_data.spiders',
        'RETRY_TIMES': 500,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429, 401],
        'ROBOTSTXT_OBEY': False
        }

    row_master = []

    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36'} 
 
    def start_requests(self):
        urls = [
            'https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050',
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)
            
    def parse(self, response):
        
        if not response.text:
            new_request_or_none = get_retry_request(
                response.request,
                spider=self,
                reason='empty',
            )
            return new_request_or_none


        data = response.text
        data = json.loads(data)
    
        for row in data['data']:
            if row['change'] > 0:
                flag = 'advance'
            elif row['change'] < 0:
                flag = 'decline'
            else:
                flag = 'unchanged'

            row_extracted = [row['lastUpdateTime'][0:11], row['symbol'], row['open'], row['dayHigh'], row['dayLow'],
                            row['lastPrice'], row['previousClose'], row['change'],
                            row['pChange'], row['totalTradedVolume'], row['totalTradedValue'],
                            row['yearHigh'], row['yearLow'], flag]
            
            self.row_master.append(row_extracted)
        
        self.df_loader()


    def df_loader(self):
        stock_attributes = ['Date', 'Symbol', 'Open', 'High', 'Low', 'LTP', 'PrevClose', 'Change', '% Change',
                            'Volume', 'Value', '52W High', '52W low', 'Flag']

        daily_df = pd.DataFrame(columns = stock_attributes)

        nifty50_row = self.row_master[0]

        for row in self.row_master[1:]:
            daily_df.loc[len(daily_df)] = row

        daily_df.sort_values(by = ['% Change'], inplace = True, ascending = False)
        daily_df.drop(range(10, 40), axis = 0, inplace = True)

        nifty50_df = pd.DataFrame([nifty50_row], columns = stock_attributes)
        self.daily_df_new = pd.concat([nifty50_df, daily_df])
        self.daily_df_new
        self.csv_loader()
    
    def csv_loader(self):

        # if file does not exist write header 
        if not os.path.isfile('NSE_daily.csv'):
            self.daily_df_new.round(2).to_csv('NSE_daily.csv', header = 'column_names', index = False)
            print("\nFinish. CSV created!\n")
        else: # else it exists so append without writing the header

            self.daily_df_new.round(2).to_csv('NSE_daily.csv', mode = 'a', header = False, index = False)
            self.csv_duplicate_checker()
        
        
    def csv_duplicate_checker(self):

        check_df = pd.read_csv('NSE_daily.csv')

        check_df.drop_duplicates(inplace = True)
        
        check_df.to_csv('NSE_daily.csv', index = False, header = 'column_names', mode = 'w')
        print("\nFinish. CSV created and duplicates dropped!\n")


if __name__ == "__main__":
  process = CrawlerProcess()
  process.crawl(NSESpider)
  process.start()