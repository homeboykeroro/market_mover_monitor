from abc import ABC, abstractmethod
from ibapi.contract import ContractDetails
from ibapi.common import BarData

class ScannerConnectorCallBack(ABC):
    @abstractmethod
    def execute_scanner_data(self, req_id: int, rank: int, contract_details: ContractDetails) -> None:
        pass
    
    @abstractmethod
    def execute_scanner_end(self, req_id: int, ticker_to_previous_close_dict: dict, scanner_connector) -> None:
        pass
    
    @abstractmethod
    def execute_historical_data(self, req_id: int, bar: BarData, ticker_to_previous_close_dict: dict) -> None:
        pass
    
    @abstractmethod
    def execute_historical_data_end(self, req_id: int, ticker_to_previous_close_dict: dict) -> None:
        pass