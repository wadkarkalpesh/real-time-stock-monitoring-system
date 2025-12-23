Real-Time Stock Monitoring System
1. Project Overview

This project is a Real-Time Stock Monitoring System developed using Python.
It implements the ETL (Extract, Transform, Load) process to fetch live stock data, analyze it, store it in a database, and display insights using a GUI.

2. Objectives

Fetch real-time stock market data

Perform ETL operations

Store processed data in a database

Execute OLAP SQL queries

Display results through a GUI dashboard

3. Technologies Used

Python

Tkinter

Pandas

Yahoo Finance API

SQLite / MySQL

SQL

4. System Workflow

Extract live stock data from Yahoo Finance

Transform data (MA, Z-Score, Volatility, Trend)

Load data into SQLite / MySQL

Perform OLAP analysis using SQL

Display results in GUI

5. Features

Live stock monitoring

Moving average and trend analysis

Buy / Sell / Hold suggestion

CSV import and export

Auto-refresh functionality

OLAP analytical queries

6. Database Used

SQLite (default local database)

MySQL (optional)

Table used: stock_data

7. Installation & Execution Steps

Install Python

Install required libraries:

pip install -r requirements.txt


Run the project:

python src/q.py

8. Sample OLAP Query
SELECT Symbol, AVG(Price) FROM stock_data GROUP BY Symbol;

9. Applications

Academic ETL and OLAP project

Real-time stock analysis

Data engineering learning system

10. Conclusion

This project demonstrates a real-time ETL-based stock monitoring system with database integration, analytical querying, and GUI visualization, making it suitable for college evaluation.