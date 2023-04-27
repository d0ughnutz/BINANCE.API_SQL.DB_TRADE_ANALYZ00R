# Get crypto API data and store in a PosgreSQL DB

This script can be used to get cryptocurrency data from public APIs for further analysis in a SQL database.

Overview of the script:
1. Get API data - cryptocurrency trade and price data from Binance.US API for trading pair BTC-USD in 1 minute candle grain
2. Structure data - Trading Pair, [Open,Close,High,Low] Price, [BTC,USD] Volume, Num Trades, Candle [Open,Close] Time
3. Load data DB - add structured data to PSQL database (must be already setup)

Note:
- SQL DB must be configured prior to running script. I used the PosgreSQL and pgadmin4 to create the DB
