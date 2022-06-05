# market_mover_monitor
PROJECT SETUP:

1. Run `py -m venv VENV_NAME`
2. Run `pip install -r requirements.txt`


HOW TO USE:

1. Download Interactive Brokers Trader Workstation (TWS) or IB Gateway
2. Download ibapi library from official website and put it in src folder
3. Set Interactive Brokers timezone to US/Eastern
4. Login Interactive Brokers account through TWS or IB Gateway (Interactive Brokers trading account with market data subscription is necessary for the program)
5. Run market_mover_monitor.py while TWS or IB Gateway is running


FUNCTION:

- This program is to facilitate day trading, finding some stocks in top gainer list to trade intra-day. Once program starts it keep scanning top gainers in pre-market or normal trading hours, depending on program start time. 1 minute candle chart is retrieved for pattern analysis. i.e stocks that are gapping up quickly and high volume ramp up.


DISCLAIMER:

- Don't try to blindly follow anyone's strategies. This program is not universal. I'm still learning how to trade and programming. This repository is to share my trading and programming knowledge only.