import os
import time
import traceback

from datasource.scanner_connector import ScannerConnector
from scanner.top_gainer_scanner_end import TopGainerScannerEnd
from scanner.closest_to_halt_scanner_end import ClosestToHaltScannerEnd
from scanner.halt_scanner_end import HaltScannerEnd

from constant.filter.scan_code import ScanCode
from constant.scanner_to_request_id import ScannerToRequestId

from utils.filter_util import get_filter
from utils.filter_util import get_top_gainer_scan_code
from utils.logger import Logger

from exception.connection_exception import ConnectionException
from exception.after_hour_reset_exception import AfterHourResetException

logger = Logger()

def main(has_after_hour_reset: bool = False):
    connector = None
    idle_msg_logged = False
    
    try:
        while True:
            gainer_scan_code = get_top_gainer_scan_code()
            
            if gainer_scan_code is not None:
                logger.log_debug_msg('Connecting...', with_speech = True, with_std_out = True)
                
                scanner_connector = ScannerConnector(has_after_hour_reset)
                scanner_connector.connect('127.0.0.1', 7496, 0)
                
                # #API Scanner subscriptions update every 30 seconds, just as they do in TWS.
                top_gainer_filter = get_filter(scan_code = gainer_scan_code, 
                                               min_price = 0.3, min_volume = 3000, 
                                               market_cap_above = None, market_cap_below = None,
                                               include_otc = False,
                                               no_of_result = 15)
                closest_to_halt_filter = get_filter(scan_code = ScanCode.CLOSEST_TO_HALT.value, 
                                                    min_price = 0.3, min_volume = 3000,
                                                    market_cap_above = None, market_cap_below = None,
                                                    include_otc = False, 
                                                    no_of_result = 10)
                halt_filter = get_filter(scan_code = ScanCode.HALTED.value, 
                                         min_price = 0.3, min_volume = 3000,
                                         market_cap_above = None, market_cap_below = None,
                                         include_otc = False, 
                                         no_of_result = 10)
                
                scanner_connector.add_scanner_connector_callback(ScannerToRequestId.TOP_GAINER.value, TopGainerScannerEnd())
                scanner_connector.add_scanner_connector_callback(ScannerToRequestId.CLOSEST_TO_HALT.value, ClosestToHaltScannerEnd())
                scanner_connector.add_scanner_connector_callback(ScannerToRequestId.HALT.value, HaltScannerEnd())
                
                scanner_connector.reqScannerSubscription(ScannerToRequestId.TOP_GAINER.value, top_gainer_filter, [], [])
                scanner_connector.reqScannerSubscription(ScannerToRequestId.CLOSEST_TO_HALT.value, closest_to_halt_filter, [], [])
                scanner_connector.reqScannerSubscription(ScannerToRequestId.HALT.value, halt_filter, [], [])
                
                scanner_connector.run()
            else:
                if not idle_msg_logged:
                    logger.log_debug_msg('Scanner is idle...', with_speech = True, with_std_out = True)
                    idle_msg_logged = True
                continue
    except Exception as e:
        has_after_hour_reset = False
        
        if connector:
            connector.disconnect()

        if isinstance(e, ConnectionException):
            sleep_time = 60

            os.system('cls')
            logger.log_error_msg(f'TWS API Connection Lost, Cause: {e}')
            logger.log_error_msg('Re-establishing Connection Due to Connectivity Issue', with_speech = True, with_std_out = True)

        elif isinstance(e, AfterHourResetException):
            sleep_time = None
            has_after_hour_reset = True

            logger.log_debug_msg('Reset Scanner Scan Code for After Hours')
            logger.log_debug_msg('After Hour Scanner Reset', with_speech = True, with_std_out = True)
        else:
            sleep_time = 10

            os.system('cls')
            logger.log_error_msg(traceback.format_exc())
            logger.log_error_msg(f'Fatal Error, Cause: {e}')
            logger.log_error_msg('Re-establishing Connection Due to Fatal Error', with_speech = True, with_std_out = True)
        
        if sleep_time:
            time.sleep(sleep_time)

        main(has_after_hour_reset)

if __name__ == '__main__':
    main()