from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.common import TickerId
from ibapi.contract import ContractDetails

from scanner.scanner_connector_callback import ScannerConnectorCallBack

from constant.request_id_prefix import RequestIdPrefix
from constant.scanner_to_request_id import ScannerToRequestId
from constant.scanner_to_timeframes import ScannerToTimeframes

from utils.logger import Logger

from exception.connection_exception import ConnectionException

logger = Logger()

class ScannerConnector(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self.__ticker_to_previous_close_dict = {}
        self.__req_id_to_callback_dict = {}
        
    def connectAck(self):
        logger.log_debug_msg('TWS Connection Success')

    def error(self, reqId: TickerId, errorCode: int, errorString: str, advancedOrderRejectJson = ""):
        ''' Callbacks to EWrapper with errorId as -1 do not represent true 'errors' but only 
        notification that a connector has been made successfully to the IB market data farms. '''
        success_error_code_list = [2104, 2105, 2106, 2108, 2158]
        ''' Error code 165 is used to by pass error of Historical Market Data Service query message:no items retrieved '''
        bypass_fatal_error_code_list = [165]
        connection_error_code_list = [1100, 1101, 1102, 2110, 2103]

        if errorCode in success_error_code_list:
            connect_success_msg = f'reqId: {reqId}, TWS Connection Success, errorCode: {errorCode}, message: {errorString}'
            logger.log_debug_msg(connect_success_msg, with_speech = False)
        elif errorCode in bypass_fatal_error_code_list:
            bypass_error_msg = f'reqId: {reqId}, By pass TWS error, errorCode: {errorCode}, message: {errorString}'
            logger.log_debug_msg(bypass_error_msg, with_speech = False)
        elif errorCode in connection_error_code_list:
            connect_fail_msg = f'reqId: {reqId}, TWS Connection Error, errorCode: {errorCode}, message: {errorString}'
            raise ConnectionException(connect_fail_msg)
        else:
            fatal_error_msg = f'reqId: {reqId}, TWS Fatal Error, errorCode: {errorCode}, message: {errorString}'
            raise Exception(fatal_error_msg)
  
    def historicalData(self, reqId, bar):
        if ((0 <= reqId - RequestIdPrefix.TOP_GAINER_DAY_CANDLE_REQ_ID_PREFIX.value <= RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1)
                or (RequestIdPrefix.TOP_GAINER_MINUTE_CANDLE_REQ_ID_PREFIX.value <= reqId <= ((RequestIdPrefix.TOP_GAINER_MINUTE_CANDLE_REQ_ID_PREFIX.value * len(ScannerToTimeframes.TOP_GAINER.value)) + RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1))):
            callback_req_id = ScannerToRequestId.TOP_GAINER.value
            
        if ((0 <= reqId - RequestIdPrefix.CLOSEST_TO_HALT_DAY_CANDLE_REQ_ID_PREFIX.value <= RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1)
                or (0 <= reqId - RequestIdPrefix.CLOSEST_TO_HALT_MINUTE_CANDLE_REQ_ID_PREFIX.value <= RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1)):
            callback_req_id = ScannerToRequestId.CLOSEST_TO_HALT.value
          
        if callback_req_id:  
            self.__req_id_to_callback_dict[callback_req_id].execute_historical_data(reqId, bar, self.__ticker_to_previous_close_dict)
        else:
            logger.log_debug_msg(f'No historical data callback is called, reqId: {reqId}', with_speech = False)

    #Marks the ending of historical bars reception.
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        if ((0 <= reqId - RequestIdPrefix.TOP_GAINER_DAY_CANDLE_REQ_ID_PREFIX.value <= RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1)
                or (RequestIdPrefix.TOP_GAINER_MINUTE_CANDLE_REQ_ID_PREFIX.value <= reqId <= ((RequestIdPrefix.TOP_GAINER_MINUTE_CANDLE_REQ_ID_PREFIX.value * len(ScannerToTimeframes.TOP_GAINER.value)) + RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1))):
            callback_req_id = ScannerToRequestId.TOP_GAINER.value
            
        if ((0 <= reqId - RequestIdPrefix.CLOSEST_TO_HALT_DAY_CANDLE_REQ_ID_PREFIX.value <= RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1)
                or (0 <= reqId - RequestIdPrefix.CLOSEST_TO_HALT_MINUTE_CANDLE_REQ_ID_PREFIX.value <= RequestIdPrefix.MAXIMUM_SCANNER_RESULT_SIZE.value - 1)):
            callback_req_id = ScannerToRequestId.CLOSEST_TO_HALT.value
        
        if callback_req_id:  
            self.__req_id_to_callback_dict[callback_req_id].execute_historical_data_end(reqId, self.__ticker_to_previous_close_dict)
        else:
            logger.log_debug_msg(f'No historical data end callback is called, reqId: {reqId}', with_speech = False)
            
        logger.log_debug_msg(f'Previous close dict: {self.__ticker_to_previous_close_dict}', with_speech = False)
        
    def scannerData(self, reqId: int, rank: int, contractDetails: ContractDetails, distance: str, benchmark: str, projection: str, legsStr: str):
        callback = self.__req_id_to_callback_dict[reqId]
        
        if callback:  
            callback.execute_scanner_data(reqId, rank, contractDetails)
        else:
            logger.log_debug_msg(f'No scanner data callback is called, reqId: {reqId}', with_speech = False)
            
    #scannerDataEnd marker will indicate when all results have been delivered.
    #The returned results to scannerData simply consists of a list of contracts, no market data field (bid, ask, last, volume, ...).
    def scannerDataEnd(self, reqId: int):
        callback = self.__req_id_to_callback_dict[reqId]
        
        if callback:  
            callback.execute_scanner_end(reqId, self.__ticker_to_previous_close_dict, self)
        else:
            logger.log_debug_msg(f'No scanner data end callback is called, reqId: {reqId}', with_speech = False)
        
    def add_scanner_connector_callback(self, reqId, callback: ScannerConnectorCallBack):
        self.__req_id_to_callback_dict[reqId] = callback