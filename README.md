# market_mover_monitor
LOCAL DEBUG SETUP:

1. Run `py -m venv VENV_NAME`
2. Go to venv directory, then execute `activate`
3. Run `pip install -r requirements.txt`
4. Run `pip install pyinstaller`


BUILD EXECUTABLE PROGRAM
1. Run `pyinstaller market_mover_monitor.py --icon=<icon_path>` to export this project as exe file


WRITE DEPENDENCY LIST TO requirements.txt:
1. Run `pip3 freeze > requirements.txt`


HOW TO USE:

1. Download Interactive Brokers Trader Workstation (TWS) or IB Gateway
2. Set Interactive Brokers timezone to US/Eastern
3. Login Interactive Brokers account through TWS or IB Gateway (Interactive Brokers trading account with market data subscription is necessary for the program)
4. Run monitor.py while TWS or IB Gateway is running


FUNCTION:

- This program is to facilitate day trading, finding some stocks in top gainer list, halted stock list to trade intra-day. Once program starts it keep scanning stocks in pre-market, normal trading hours or after hours, depending on program start time. 1 minute candles will be retrieved for analysis. The text to speech engine will read the result so that traders don't need to manually keep monitoring stock scanner all the day.

    - Stocks that are gapping up
    - Stocks that are ramping up
    - Stocks that are near previous high or reaching new high
    - Stocks that are closest to limit up down
    - Stocks that are halting


DISCLAIMER:

- Don't try to blindly follow anyone's strategies. This program is not universal. I'm still learning how to trade and programming. This repository is to share my trading and programming knowledge only.