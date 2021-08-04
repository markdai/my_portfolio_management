# Investment Management

Python package to manage Equity and Fixed Income transactions. 

## Getting Started

* `src/` contains all basic modules for this package.
	* `logger.py` the logger constructor;
	* `FinAPI_utility.py` the Yahoo Finance API connector;
	* `eq_SQLite_utility.py` the SQLite connector for Equity;
	* `fixed_SQLite_utility.py`  the SQLite connector for Fixed Income;
	* `equity.py` the management module for Equity;
	* `fixed_income.py` the management module for Fixed Income;
	* `overview_generator.py` the generator for Allocation Reports;
* `test/` contains UnitTest for some basic modules.
	* `test_FinAPI_utility.py`
	* `test_eq_SQLite_utility.py`
	* `test_fixed_SQLite_utility.py`
* `templates/` contains SQLite Table Schema and View Query.
	* `equity_tables_schema.json`
	* `fixed_tables_schema.json`
	* `equity_positions_view_query.sql`
	* `fixed_positions_view_query.sql`
* `databases/` contains SQLite database instances.
	* `equity.db`
	* `fixed_income.db`
	* `cash_equivalent.csv`
* `backup/` contains backup for :table: transaction in comma delimited CSV format.
	* equity_transaction_backup_YYYYMMDD.csv
	* fixed_transaction_backup_YYYYMMDD.csv
* `logs/` contains execution logs.
* `snapshots/` contains investment overview snapshot.
	* snapshot_YYYYMMDD.html
* `main.py` it is the master script for this package.


## Prerequisites

python v3.6+  
pip

### Installing

pip3 install pandas  
pip3 install lxml  
pip3 install html5lib
pip3 install yfinance


## How to run

    To get help information:
        python main.py -h

    To generate Overview Snapshot:
        python main.py overview

    To manage equity investment Database:
        python main.py equity -m update
        python main.py equity -m backup
        python main.py equity -m restore
        python main.py equity -m add -ee 'APPL,BUY,2099-99-99,250.0,10,ETF,TD,Apple Inc'

    To manage fixed income investment Database:
        python main.py fixed -m backup
        python main.py fixed -m restore
        python main.py fixed -m add -fe 'US Treasury Notes,XXXXXXXX1,TREASURY,10,100.0,2018-12-31,2019-12-31,1000.0,Trading Center,YTM=0.025'
     