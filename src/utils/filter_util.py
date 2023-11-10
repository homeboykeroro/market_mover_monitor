import pytz
import datetime

from ibapi.scanner import ScannerSubscription

from constant.instrument import Instrument
from constant.filter.scan_code import ScanCode

from utils.logger import Logger

logger = Logger()

def get_top_gainer_scan_code():
    # Get current datetime in HK time
    hk_datetime = datetime.datetime.now()
    
    # Convert Hong Kong time to US/Eastern time
    us_eastern_timezone = pytz.timezone('US/Eastern')
    hong_kong_timezone = pytz.timezone('Asia/Hong_Kong')
    hk_datetime = hong_kong_timezone.localize(hk_datetime)
    us_time = hk_datetime.astimezone(us_eastern_timezone)

    # Define trading hours in US/Eastern time
    pre_market_trading_hour_start_time = datetime.time(4, 0, 0)
    normal_trading_hour_start_time = datetime.time(9, 30, 0)
    normal_trading_hour_end_time = datetime.time(16, 0, 0)
    after_hours_trading_hour_end_time = datetime.time(20, 0, 0)

    # Check which trading period the US time falls into
    if pre_market_trading_hour_start_time <= us_time.time() < normal_trading_hour_start_time:
        logger.log_debug_msg('Pre-market trading hours', with_speech = False)
        return ScanCode.TOP_GAINERS.value
    elif normal_trading_hour_start_time <= us_time.time() < normal_trading_hour_end_time:
        logger.log_debug_msg('Normal trading hours', with_speech = False)
        return ScanCode.TOP_GAINERS.value
    elif normal_trading_hour_end_time <= us_time.time() < after_hours_trading_hour_end_time:
        logger.log_debug_msg('After hours trading hours', with_speech = False)
        return ScanCode.TOP_GAINERS_IN_AFTER_HOURS.value
    else:
        return None

def get_filter(
            scan_code: ScanCode, 
            min_price: float, min_volume: int, 
            market_cap_above: float, market_cap_below: float,
            include_otc: bool,
            no_of_result: int,
            instrument: str = Instrument.STOCKS.value) -> ScannerSubscription:
    scanner_filter = ScannerSubscription()

    scanner_filter.scanCode = scan_code
    scanner_filter.instrument = instrument

    if not include_otc:
        scanner_filter.locationCode = 'STK.US.MAJOR'
    else:
        scanner_filter.locationCode = 'STK.US'

    if min_price:
        scanner_filter.abovePrice = min_price
        
    if min_volume:
        scanner_filter.aboveVolume = min_volume
        
    if market_cap_above:
        scanner_filter.marketCapAbove = market_cap_above
        
    if market_cap_below:
        scanner_filter.marketCapBelow = market_cap_below
    
    #Maximum no. of rows is 50, no_of_result shouldn't exceed 50
    scanner_filter.numberOfRows = no_of_result

    return scanner_filter