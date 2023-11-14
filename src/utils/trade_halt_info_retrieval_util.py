import requests
from bs4 import BeautifulSoup
import datetime
import pytz
import re
import time

from model.trade_halt_record import TradeHaltRecord

from utils.logger import Logger

RSS_LINK = 'http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts'
REFRESH_TIME = 30

logger = Logger()

def retrieve_trade_halt_info() -> dict:
    logger.log_debug_msg(f'Retrieve trade halt info from Nasdaq at {datetime.datetime.now().astimezone(pytz.timezone("US/Eastern"))}')
    ticker_to_trade_halt_info_dict = {}
    session = requests.Session()
    
    try:
        startime = time.time()
        rss_feed_response = session.get(RSS_LINK)
         # Raises a HTTPError if the response status is 4xx, 5xx
        rss_feed_response.raise_for_status() 
    except Exception as request_exception:
        logger.log_error_msg(f'An error occurred while making the request: {request_exception}')
    else:
        try:
            feed_contents = rss_feed_response.text
            logger.log_debug_msg(f'Trade halt RSS feed contents retrieval time: {time.time() - startime}')
            soup = BeautifulSoup(feed_contents, 'lxml')
            trade_halt_record_list = soup.find_all('item')
            
            for record in trade_halt_record_list:
                symbol = record.find(TradeHaltRecord.SYMBOL).string
                
                if re.match('^[a-zA-Z]{1,4}$', symbol): 
                    company = record.find(TradeHaltRecord.COMPANY).string
                    reason = record.find(TradeHaltRecord.REASON).string
                    halt_date = record.find(TradeHaltRecord.HALT_DATE).string
                    halt_time = record.find(TradeHaltRecord.HALT_TIME).string
                    resumption_quote_time = record.find(TradeHaltRecord.RESUMPTION_QUOTE_TIME).string
                    resumption_trade_time = record.find(TradeHaltRecord.RESUMPTION_TRADE_TIME).string

                    trade_halt_record = TradeHaltRecord(symbol, company, reason, halt_date, halt_time, resumption_quote_time, resumption_trade_time)
                    ticker_to_trade_halt_info_dict[symbol] = trade_halt_record
                else:
                    logger.log_debug_msg(f'Exclude invalid ticker of {symbol} from trade halt info result')      
                
        except Exception as parse_content_exception:
            logger.log_error_msg(f'An error occurred while dealing with trade halt info: {parse_content_exception}')
    finally:
        return ticker_to_trade_halt_info_dict
    
    
    

