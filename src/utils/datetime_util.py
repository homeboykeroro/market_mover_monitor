from dateutil.parser import parse
import pytz
import datetime

def convert_to_datetime(date_string: str, timezone: str):
    # This function will convert a date string in any format to a datetime object
    dt = parse(date_string)
    tz = pytz.timezone(timezone)
    return dt.astimezone(tz)

def calculate_difference_in_minutes(datetime1: datetime, datetime2: datetime):
    # This function will calculate the difference between two datetime objects in minutes
    difference = datetime1 - datetime2
    return int(difference.total_seconds() / 60)

def get_trading_start_time_by_current_datetime(input_current_datetime: datetime) -> datetime:
    # Convert Hong Kong time to US/Eastern time
    us_eastern_timezone = pytz.timezone('US/Eastern')
    hong_kong_timezone = pytz.timezone('Asia/Hong_Kong')
    hk_datetime = hong_kong_timezone.localize(input_current_datetime)
    us_time = hk_datetime.astimezone(us_eastern_timezone)

    # Define trading hours in US/Eastern time
    pre_market_trading_hour_start_time = datetime.time(4, 0, 0)
    normal_trading_hour_start_time = datetime.time(9, 30, 0)
    normal_trading_hour_end_time = datetime.time(16, 0, 0)
    after_hours_trading_hour_end_time = datetime.time(20, 0, 0)
    
     # Check which trading period the US time falls into
    if pre_market_trading_hour_start_time <= us_time.time() < normal_trading_hour_start_time:
        return pre_market_trading_hour_start_time
    elif normal_trading_hour_start_time <= us_time.time() < normal_trading_hour_end_time:
        return normal_trading_hour_start_time
    elif normal_trading_hour_end_time <= us_time.time() < after_hours_trading_hour_end_time:
        return after_hours_trading_hour_end_time
    else:
        return None