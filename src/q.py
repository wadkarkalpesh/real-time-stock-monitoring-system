import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import yfinance as yf
import pandas as pd
import threading
import datetime
import csv
import sqlite3
import requests
import os
import sys

# Attempt to import MySQL connector early and provide a warning
try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False
    # Set up a dummy module for type hints to avoid errors during class definition
    class DummyMySQL:
        def connect(*args, **kwargs): return None
        def Error(*args, **kwargs): return Exception
    mysql.connector = DummyMySQL()

# ===================== Transformer =====================
class Transformer:
    """Handles data transformations including MA, Z-Score, Volatility, and Trend analysis."""
    def transform(self, df):
        # 1. Standardize Change Column and ensure numeric type (Data Cleaning)
        if "Change" in df.columns:
            df = df.rename(columns={"Change": "ChangePct"})
        if "ChangePct" in df.columns:
            df["ChangePct"] = pd.to_numeric(df["ChangePct"], errors="coerce").fillna(0)

        # 2. Financial and Statistical Features (requires 'Price')
        if "Price" in df.columns:
            # [cite_start]Moving Average (MA5, MA10): Short & long-term stock trend smoothing [cite: 26]
            df["MA5"] = df["Price"].rolling(window=5, min_periods=1).mean().round(4)
            df["MA10"] = df["Price"].rolling(window=10, min_periods=1).mean().round(4)
            
            # [cite_start]Z-Score Normalization: Deviation of stock price from mean [cite: 27]
            mean_price = df["Price"].mean()
            std_price = df["Price"].std() if df["Price"].std() != 0 else 1
            df["ZScore"] = ((df["Price"] - mean_price) / std_price).round(4)

        # 3. Categorical Volatility
        def classify_volatility(change):
            # [cite_start]Volatility Classification: Low, Medium, High based on percentage change [cite: 28]
            if abs(change) > 5:
                return "High"
            elif abs(change) > 2:
                return "Medium"
            else:
                return "Low"

        if "ChangePct" in df.columns:
            df["Volatility"] = df["ChangePct"].apply(classify_volatility)
        else:
             df["Volatility"] = "N/A" # Default if ChangePct is missing

        # 4. Final Aggregation and Trend/Suggestion Calculation
        transformed = []
        for _, row in df.iterrows():
            symbol = str(row.get("Symbol", "N/A")).upper()
            price = row.get("Price", 0)
            change = row.get("ChangePct", 0)
            
            # [cite_start]Suggestion Column: Buy, Sell, or Hold (Derived Business Insight) [cite: 35]
            suggestion = "Buy" if change > 0.5 else "Sell" if change < -0.5 else "Hold" # Small buffer for hold
            
            ma5 = row.get("MA5", 0)
            ma10 = row.get("MA10", 0)
            zscore = row.get("ZScore", 0)
            volatility = row.get("Volatility", "N/A")
            
            # [cite_start]Trend Analysis: Uptrend / Downtrend / Sideways based on MA5 vs MA10 [cite: 29]
            trend = "Uptrend" if ma5 > ma10 else "Downtrend" if ma5 < ma10 else "Sideways"
            
            ts = datetime.datetime.now()
            # [cite_start]Timestamping: Day & Hour of record [cite: 36]
            timestamp = ts.strftime("%Y-%m-%d %H:%M:%S")
            day = ts.strftime("%A")
            hour = ts.hour

            transformed.append((
                symbol, price, round(change, 4), suggestion, timestamp,
                round(ma5, 4), round(ma10, 4), volatility, trend,
                round(zscore, 4), day, hour
            ))
            
        return pd.DataFrame(transformed, columns=[
            "Symbol","Price","ChangePct","Suggestion","Timestamp",
            "MA5","MA10","Volatility","Trend","ZScore","Day","Hour"
        ])

    # Extra Transformations
    def add_daily_return(self, df):
        # [cite_start]Daily Return: Percentage return compared to previous day [cite: 31]
        if "Price" in df.columns:
            df["DailyReturn"] = df["Price"].pct_change().fillna(0).round(4) * 100
        return df

    def fill_missing_values(self, df):
        # [cite_start]Handling missing values using forward-fill (ffill) and backward-fill (bfill) [cite: 22]
        if "Price" in df.columns:
            df["Price"] = pd.to_numeric(df["Price"], errors='coerce')
        # FIXED: Replacing deprecated fillna(method=...) with ffill() and bfill()
        return df.ffill().bfill()

    def normalize_price(self, df):
        # [cite_start]Price Normalization: Min-Max scaling to range [0,1] [cite: 32]
        if "Price" in df.columns:
            min_val = df["Price"].min()
            max_val = df["Price"].max()
            denom = (max_val - min_val) if (max_val - min_val) != 0 else 1
            df["PriceNormalized"] = ((df["Price"] - min_val) / denom).round(4)
        return df

# ===================== Database (SQLite) =====================
class Database:
    """SQLite Database handling for local persistent storage."""
    def __init__(self, db_name="stocks.db"):
        # [cite_start]SQLite (Local) default persistent storage [cite: 38]
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.create_table()

    def create_table(self):
        # [cite_start]Final Database Table Description (stock_data) [cite: 41]
        query = """
        CREATE TABLE IF NOT EXISTS stock_data (
            Symbol TEXT, Price REAL, ChangePct REAL, Suggestion TEXT,
            Timestamp TEXT, MA5 REAL, MA10 REAL, Volatility TEXT,
            Trend TEXT, ZScore REAL, Day TEXT, Hour INTEGER, DataSource TEXT
        )
        """
        self.conn.execute(query)
        self.conn.commit()

    def insert_rows(self, rows, source="local"):
        # [cite_start]Source of data (live_yahoo / csv_file) [cite: 80]
        rows_with_source = [tuple(r) + (source,) for r in rows]
        try:
            self.conn.executemany("""
                INSERT INTO stock_data
                (Symbol, Price, ChangePct, Suggestion, Timestamp,
                 MA5, MA10, Volatility, Trend, ZScore, Day, Hour, DataSource)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows_with_source)
            self.conn.commit()
        except Exception as e:
            # Added logging for debugging
            print(f"SQLite insertion error: {e}", file=sys.stderr)

    def query(self, sql):
        try:
            # pandas.read_sql_query expects a connection object
            df = pd.read_sql_query(sql, self.conn)
            return df
        except Exception as e:
            messagebox.showerror("SQLite Query Error", str(e))
            return pd.DataFrame()

    def close(self):
        if self.conn:
            self.conn.close()

# ===================== MySQL integration (optional) =====================
class MySQLDatabase:
    """MySQL Database handling for external persistent storage."""
    def __init__(self, host="127.0.0.1", user="root", password="root", database="stock_etl"):
        # [cite_start]MySQL (Optional, if configured) external persistent storage [cite: 39]
        if not MYSQL_AVAILABLE:
            self.conn = None
            self.cursor = None
            return

        self.mysql = mysql.connector
        self.conn = None
        self.cursor = None
        try:
            # 1. Connect to server and create database if it doesn't exist
            tmp = self.mysql.connect(host=host, user=user, password=password)
            cur = tmp.cursor()
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{database}`")
            tmp.commit()
            cur.close()
            tmp.close()

            # 2. Connect to the new/existing database and create table
            self.conn = self.mysql.connect(host=host, user=user, password=password, database=database)
            self.cursor = self.conn.cursor()
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Symbol VARCHAR(50),
                Price DECIMAL(18,4),
                ChangePct DECIMAL(10,4),
                Suggestion VARCHAR(10),
                Timestamp DATETIME,
                MA5 DECIMAL(18,4),
                MA10 DECIMAL(18,4),
                Volatility VARCHAR(20),
                Trend VARCHAR(50),
                ZScore DECIMAL(10,4),
                Day VARCHAR(20),
                Hour INT,
                DataSource VARCHAR(20)
            )
            """)
            self.conn.commit()
        except self.mysql.Error as e:
            print("MySQL init error:", e)
            messagebox.showwarning("MySQL Warning", f"Could not connect to MySQL server. Functionality disabled. Error: {e}")
            self.conn = None
            self.cursor = None

    def insert_rows(self, rows, source="live"):
        if not getattr(self, "cursor", None):
            return
        # Query expects 13 placeholders (%s) matching the 13 columns (excluding auto-increment ID)
        query = """
        INSERT INTO stock_data
        (Symbol, Price, ChangePct, Suggestion, Timestamp, MA5, MA10, Volatility, Trend, ZScore, Day, Hour, DataSource)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        data = []
        # Ensure 13 items are passed: 12 from Transformer + 1 for DataSource
        for r in rows:
            data.append((r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11], source))
        try:
            self.cursor.executemany(query, data)
            self.conn.commit()
        except self.mysql.Error as e:
            # Log insertion errors (including "Unknown column" if data type mismatch occurs)
            print(f"MySQL insert error: {e}", file=sys.stderr)

    def query(self, sql):
        if not getattr(self, "conn", None):
            messagebox.showwarning("MySQL Query Warning", "MySQL connection is not established.")
            return pd.DataFrame()
        try:
            df = pd.read_sql_query(sql, self.conn)
            return df
        except self.mysql.Error as e:
            messagebox.showerror("MySQL Query Error", str(e))
            return pd.DataFrame()

    def close(self):
        if getattr(self, "cursor", None):
            try: self.cursor.close()
            except: pass
        if getattr(self, "conn", None):
            try: self.conn.close()
            except: pass

# ===================== Nifty 50 Helper =====================
def get_nifty50_symbols_online():
    """Fetches current Nifty 50 symbols from NSE India."""
    # [cite_start]Fetching live stock data of Nifty 50 companies [cite: 9]
    url = "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com"
    }
    with requests.Session() as session:
        session.headers.update(headers)
        try:
            session.get("https://www.nseindia.com", timeout=5) # Warm-up/Cookie acquisition
            resp = session.get(url, timeout=10)
            resp.raise_for_status() # Raise HTTPError for bad responses
            data = resp.json()
            # Append ".NS" for Yahoo Finance compatibility
            symbols = [item['symbol'] + ".NS" for item in data.get('data', [])]
            # Ensure index symbol itself is not included, as it often fails download
            symbols = [s for s in symbols if s not in ['NIFTY 50.NS', '^NSEI']] 
            return symbols
        except Exception as e:
            print(f"Error fetching Nifty 50 symbols: {e}", file=sys.stderr)
            return []

# ===================== ETL App =====================
class ETLApp:
    def __init__(self, root):
        self.root = root
        self.transformer = Transformer()
        self.db = Database()
        self.mysql_db = MySQLDatabase()
        
        # Initialize DataFrames to avoid errors on first transformation attempts
        self.yahoo_data = pd.DataFrame()
        self.csv_data = pd.DataFrame()
        self.yahoo_transformed = pd.DataFrame()
        self.csv_transformed = pd.DataFrame()
        
        self.refresh_job = None
        self.auto_refresh_var = tk.BooleanVar(value=False)
        
        self.setup_ui()
        
        if not self.mysql_db.conn and MYSQL_AVAILABLE:
            self.log("❗ MySQL functionality is disabled due to connection error.", level='warning')
        elif not MYSQL_AVAILABLE:
            self.log("❗ MySQL connector not found. MySQL functionality is disabled.", level='warning')

    # ----- UI -----
    def setup_ui(self):
        self.root.title("🧩 ETL + OLAP Dashboard")
        self.root.geometry("1400x1200")

        # --- DARK THEME COLOR PALETTE ---
        DARK_BG = "#2E2E2E"       # Main background
        DARK_FG = "#FFFFFF"       # Main foreground (text)
        DARK_WIDGET_BG = "#404040" # Background for frames and entry
        DARK_HIGHLIGHT = "#00BFA5" # Highlight color (Teal/Cyan)
        
        self.root.configure(bg=DARK_BG)

        style = ttk.Style()
        style.theme_use('clam') # Use the 'clam' theme as a base

        # Configure General Styles and Notebook
        style.configure("TNotebook", background=DARK_BG, borderwidth=0)
        style.configure("TNotebook.Tab", background=DARK_WIDGET_BG, foreground=DARK_FG, padding=[15, 5])
        style.map("TNotebook.Tab", background=[("selected", DARK_HIGHLIGHT)])
        style.configure(".", background=DARK_BG, foreground=DARK_FG) # General widget background/foreground

        # Configure Treeview (Data Display)
        style.configure("Treeview", 
                        background=DARK_WIDGET_BG, 
                        foreground=DARK_FG,
                        fieldbackground=DARK_WIDGET_BG,
                        rowheight=25,
                        borderwidth=0)
        style.configure("Treeview.Heading", 
                        background=DARK_WIDGET_BG, 
                        foreground=DARK_HIGHLIGHT, 
                        font=('Arial', 10, 'bold'))
        style.map('Treeview', 
                  background=[('selected', DARK_HIGHLIGHT)]) # Highlight selected rows

        # Configure Buttons (ttk.Button style applied via TButton)
        style.configure("TButton", 
                        background=DARK_WIDGET_BG, 
                        foreground=DARK_FG, 
                        relief="flat", 
                        padding=6)
        style.map("TButton", 
                 background=[('active', DARK_HIGHLIGHT)], 
                 foreground=[('active', DARK_BG)])
        
        # Configure Checkbuttons and other minor widgets
        style.configure("TCheckbutton", background=DARK_BG, foreground=DARK_FG)
        
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill="both", expand=True, padx=10, pady=10)

        etl_frame = tk.Frame(notebook, bg=DARK_BG)
        notebook.add(etl_frame, text="ETL Dashboard")

        # Buttons and Controls Frame
        control_frame = tk.Frame(etl_frame, bg=DARK_BG)
        control_frame.pack(fill="x", pady=5)
        
        # ETL Buttons
        btn_frame = tk.Frame(control_frame, bg=DARK_BG)
        btn_frame.pack(side="left", padx=10)
        tk.Button(btn_frame, text="📈 Fetch Yahoo Finance", bg="#4CAF50", fg="white",
                  command=self.fetch_yahoo).grid(row=0, column=0, padx=5, pady=5)
        tk.Button(btn_frame, text="📂 Load CSV", bg="#2196F3", fg="white",
                  command=self.load_csv_dialog).grid(row=0, column=1, padx=5, pady=5)
        tk.Button(btn_frame, text="💾 Save CSV", bg="#FF9800", fg="white",
                  command=self.save_csv).grid(row=0, column=2, padx=5, pady=5)

        # Auto-refresh controls
        refresh_frame = tk.Frame(control_frame, bg=DARK_BG)
        refresh_frame.pack(side="right", padx=10)
        tk.Checkbutton(refresh_frame, text="Auto-refresh (Live)", variable=self.auto_refresh_var,
                       command=self.toggle_auto_refresh, bg=DARK_BG, fg=DARK_FG, selectcolor=DARK_WIDGET_BG).grid(row=0, column=0, padx=8)
        tk.Label(refresh_frame, text="Interval (s):", bg=DARK_BG, fg=DARK_FG).grid(row=0, column=1, padx=(10,0))
        self.refresh_interval_spin = tk.Spinbox(refresh_frame, from_=10, to=3600, width=5, bg=DARK_WIDGET_BG, fg=DARK_FG, buttonbackground=DARK_WIDGET_BG)
        self.refresh_interval_spin.delete(0, tk.END)
        self.refresh_interval_spin.insert(0, 60) # Default 60 seconds
        self.refresh_interval_spin.grid(row=0, column=2, padx=4)

        # Extra Transform Buttons
        extra_btn_frame = tk.Frame(etl_frame, bg=DARK_BG)
        extra_btn_frame.pack(pady=5)
        
        # Yahoo transforms
        tk.Label(extra_btn_frame, text="Live Transforms:", bg=DARK_BG, fg=DARK_FG).grid(row=0, column=0, padx=(5,0), sticky="w")
        tk.Button(extra_btn_frame, text="📊 Add Daily Return", bg="#009688", fg="white",
                  command=self.apply_yahoo_daily_return).grid(row=0, column=1, padx=5)
        tk.Button(extra_btn_frame, text="🩹 Fill Missing Values", bg="#795548", fg="white",
                  command=self.apply_yahoo_fill_missing).grid(row=0, column=2, padx=5)
        tk.Button(extra_btn_frame, text="⚖ Normalize Price", bg="#607D8B", fg="white",
                  command=self.apply_yahoo_normalize).grid(row=0, column=3, padx=5)
        
        # CSV transforms
        tk.Label(extra_btn_frame, text="CSV Transforms:", bg=DARK_BG, fg=DARK_FG).grid(row=0, column=4, padx=(15,0), sticky="w")
        tk.Button(extra_btn_frame, text="📊 Add Daily Return", bg="#009688", fg="white",
                  command=self.apply_csv_daily_return).grid(row=0, column=5, padx=5)
        tk.Button(extra_btn_frame, text="🩹 Fill Missing Values", bg="#795548", fg="white",
                  command=self.apply_csv_fill_missing).grid(row=0, column=6, padx=5)
        tk.Button(extra_btn_frame, text="⚖ Normalize Price", bg="#607D8B", fg="white",
                  command=self.apply_csv_normalize).grid(row=0, column=7, padx=5)


        # Treeviews containers
        tree_container = tk.Frame(etl_frame, bg=DARK_BG)
        tree_container.pack(fill="both", expand=True, padx=10)
        
        # Yahoo Panel
        yahoo_panel = tk.Frame(tree_container, bg=DARK_BG)
        yahoo_panel.pack(side="left", fill="both", expand=True, padx=(0, 5))
        tk.Label(yahoo_panel, text="Yahoo Finance Raw Data:", bg=DARK_BG, fg=DARK_FG).pack(pady=(5,0))
        self.tree_yahoo_raw = ttk.Treeview(yahoo_panel, show="headings", height=8)
        self.tree_yahoo_raw.pack(fill="both", expand=True, pady=2)
        tk.Label(yahoo_panel, text="Yahoo Finance Transformed Data (DB Ready):", bg=DARK_BG, fg=DARK_FG).pack(pady=(5,0))
        self.tree_yahoo_transformed = ttk.Treeview(yahoo_panel, show="headings", height=8)
        self.tree_yahoo_transformed.pack(fill="both", expand=True, pady=2)

        # CSV Panel
        csv_panel = tk.Frame(tree_container, bg=DARK_BG)
        csv_panel.pack(side="right", fill="both", expand=True, padx=(5, 0))
        tk.Label(csv_panel, text="CSV Raw Data:", bg=DARK_BG, fg=DARK_FG).pack(pady=(5,0))
        self.tree_csv_raw = ttk.Treeview(csv_panel, show="headings", height=8)
        self.tree_csv_raw.pack(fill="both", expand=True, pady=2)
        tk.Label(csv_panel, text="CSV Transformed Data (DB Ready):", bg=DARK_BG, fg=DARK_FG).pack(pady=(5,0))
        self.tree_csv_transformed = ttk.Treeview(csv_panel, show="headings", height=8)
        self.tree_csv_transformed.pack(fill="both", expand=True, pady=2)


        # Log
        tk.Label(etl_frame, text="Event Log:", bg=DARK_BG, fg=DARK_FG).pack(fill="x", padx=10, pady=(5,0))
        self.log_text = tk.Text(etl_frame, height=6, bg="#1E1E1E", fg="#FFFFFF")
        self.log_text.pack(fill="x", padx=10, pady=5)

        # OLAP Tab
        olap_frame = tk.Frame(notebook, bg=DARK_BG)
        notebook.add(olap_frame, text="OLAP Queries")
        
        query_frame = tk.Frame(olap_frame, bg=DARK_BG)
        query_frame.pack(fill="x", pady=5, padx=10)
        
        tk.Label(query_frame, text="SQL Query:", bg=DARK_BG, fg=DARK_FG).pack(side="left", padx=5)
        self.query_entry = tk.Entry(query_frame, width=100, bg=DARK_WIDGET_BG, fg=DARK_FG, insertbackground=DARK_FG)
        # [cite_start]OLAP-style query (Problem Statement 1) [cite: 83, 84]
        self.query_entry.insert(0, "SELECT Symbol, Volatility, Trend, AVG(Price) as AvgPrice, COUNT(*) as Count FROM stock_data GROUP BY Symbol, Volatility, Trend ORDER BY Count DESC")
        self.query_entry.pack(side="left", fill="x", expand=True, padx=5)
        
        tk.Button(query_frame, text="Run Query (SQLite)", bg="#673AB7", fg="white", command=self.run_query_sqlite).pack(side="left", padx=5)
        tk.Button(query_frame, text="Run Query (MySQL)", bg="#4E342E", fg="white", command=self.run_query_mysql).pack(side="left", padx=5)
        
        self.query_tree = ttk.Treeview(olap_frame, show="headings", height=20)
        self.query_tree.pack(fill="both", expand=True, padx=10, pady=10)


    # ----- Helper functions -----
    def log(self, msg, level='info'):
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        tag = ""
        # Set colors for warnings/errors in the log area
        if level == 'warning':
            tag = "WARNING"
            self.log_text.tag_config(tag, foreground="#FFC107") # Amber for warning
        elif level == 'error':
            tag = "ERROR"
            self.log_text.tag_config(tag, foreground="#F44336") # Red for error
        
        self.log_text.insert(tk.END, f"[{ts}] {msg}\n", tag if tag else ())
        self.log_text.see(tk.END)

    def tree_to_df(self, tree):
        """Converts a Treeview's contents back into a DataFrame."""
        rows = [tree.item(i)["values"] for i in tree.get_children()]
        if not rows:
            return pd.DataFrame()
        cols = list(tree["columns"])
        df = pd.DataFrame(rows, columns=cols)
        
        numeric_cols = ["Price", "ChangePct", "MA5", "MA10", "ZScore", "DailyReturn", "Hour", "PriceNormalized"]
        for c in numeric_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c].astype(str).str.replace(',', '', regex=False), errors="coerce")
        return df

    def show_transform_changes(self, before: pd.DataFrame, after: pd.DataFrame, label: str):
        # Helper function for logging transformation impact
        if before is None: before = pd.DataFrame()
        if after is None: after = pd.DataFrame()
        
        added = list(set(after.columns) - set(before.columns))
        
        if "DailyReturn" in added:
            count = after["DailyReturn"].fillna(0).apply(lambda x: abs(x) > 1e-4).sum()
            self.log(f"🟢 {label}: DailyReturn added. {count} rows non-zero.")
        elif "PriceNormalized" in added and "Price" in before.columns:
            pmin, pmax = before["Price"].min(), before["Price"].max()
            self.log(f"🟢 {label}: PriceNormalized added. Price min/max: {pmin:.2f}/{pmax:.2f}")
        elif "Fill Missing" in label and not before.empty:
            filled_count = 0
            if "Price" in before.columns and "Price" in after.columns:
                 filled_count = before["Price"].isna().sum() - after["Price"].isna().sum()
            self.log(f"🟢 {label}: Missing values filled. Estimated Price NaNs filled: {filled_count}")
        else:
             self.log(f"🟢 {label}: Transformation applied. Shape: {after.shape}")


    def populate_treeview(self, tree, df):
        # Clear existing data
        tree.delete(*tree.get_children())
        
        if df.empty:
            tree["columns"] = []
            return

        # Configure columns and headings
        cols = list(df.columns)
        tree["columns"] = cols
        
        # Set column width and anchor
        for col in cols:
            tree.heading(col, text=col)
            width = max(len(col) * 10, 80) 
            if col in ["Symbol", "Suggestion", "Trend"]: width = 120
            if col in ["Timestamp", "DataSource"]: width = 160
            tree.column(col, width=width, anchor="w")

        # Insert data
        for _, row in df.iterrows():
            # Format numbers for better display in the UI
            values = [f"{val:.4f}" if isinstance(val, (float, int)) and not col.endswith("Hour") else str(val) for col, val in row.items()]
            tree.insert("", "end", values=values)

    # ----- ETL Methods (E: Extract) -----
    def fetch_yahoo(self):
        def _fetch():
            self.log("🟡 Fetching Nifty 50 symbols...")
            symbols = get_nifty50_symbols_online()
            if not symbols:
                # Fallback to a few reliable symbols if the online fetch fails
                symbols = ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS"] 
                self.log("⚠️ Failed to get live Nifty 50. Using hardcoded symbols.", level='warning')

            self.log(f"🟡 Fetching live data for {len(symbols)} stocks...")
            
            # [cite_start]FIXED: Added auto_adjust=True to silence FutureWarning and ensure correct price data [cite: 6]
            data = yf.download(tickers=symbols, period="5d", interval="1d", progress=False, auto_adjust=True) 
            
            # Check for failed download symbols
            if isinstance(data.columns, pd.MultiIndex):
                valid_symbols = data.columns.get_level_values(1).unique().tolist()
            else:
                # Single symbol download returns a non-multi-index DF
                valid_symbols = symbols if not data.empty else []

            if data.empty or not valid_symbols:
                self.log("❌ Failed to download Yahoo Finance data.", level='error')
                # Reset data to empty if failed
                self.yahoo_data = pd.DataFrame()
                self.yahoo_transformed = pd.DataFrame()
                self.populate_treeview(self.tree_yahoo_raw, self.yahoo_data)
                self.populate_treeview(self.tree_yahoo_transformed, self.yahoo_transformed)
                return
            
            raw_data = []
            
            for symbol in valid_symbols:
                try:
                    # Access the latest available data point for each symbol
                    if isinstance(data.columns, pd.MultiIndex):
                         latest = data.xs(symbol, level=1, axis=1).iloc[-1]
                         previous = data.xs(symbol, level=1, axis=1).iloc[-2]
                    else:
                         latest = data.iloc[-1]
                         previous = data.iloc[-2]

                    # Calculate ChangePct
                    close = latest["Close"]
                    prev_close = previous["Close"]
                    change_pct = ((close - prev_close) / prev_close) * 100 if prev_close else 0

                    raw_data.append({
                        "Symbol": symbol,
                        "Price": close,
                        "ChangePct": change_pct,
                        "Volume": latest.get("Volume", 0)
                    })
                except Exception as e:
                    print(f"Skipping failed symbol {symbol}: {e}", file=sys.stderr)
                    continue

            self.yahoo_data = pd.DataFrame(raw_data)
            self.populate_treeview(self.tree_yahoo_raw, self.yahoo_data)
            self.log(f"✅ Yahoo Finance raw data extracted for {len(self.yahoo_data)} stocks.")
            
            # Automatically apply base transform and load
            self.apply_yahoo_base_transform()

        # Run extraction in a separate thread
        threading.Thread(target=_fetch).start()
        
    def load_csv_dialog(self):
        path = filedialog.askopenfilename(defaultextension=".csv",
                                          filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
        if path:
            self.log(f"🟡 Loading CSV from: {os.path.basename(path)}")
            try:
                # [cite_start]CSV File Extraction → Example: stocks.csv [cite: 17]
                df = pd.read_csv(path)
                # Ensure Symbol and Price columns exist
                if 'Symbol' not in df.columns or 'Price' not in df.columns:
                    messagebox.showwarning("CSV Error", "CSV must contain 'Symbol' and 'Price' columns.")
                    self.log("❌ CSV file is missing 'Symbol' or 'Price' columns.", level='error')
                    return

                self.csv_data = df.copy()
                self.populate_treeview(self.tree_csv_raw, self.csv_data)
                self.log(f"✅ CSV file loaded successfully. Rows: {len(self.csv_data)}")
                
                # Automatically apply base transform and load
                self.apply_csv_base_transform()

            except Exception as e:
                messagebox.showerror("File Error", f"Could not read CSV file: {e}")
                self.log(f"❌ Error loading CSV: {e}", level='error')
    
    # ----- ETL Methods (T & L: Transform and Load) -----
    
    def _apply_transform_and_load(self, raw_df, tree_transformed, source_name):
        if raw_df.empty:
            self.log(f"⚠️ Cannot apply transform: {source_name} raw data is empty.", level='warning')
            return pd.DataFrame()

        self.log(f"🟡 Applying base ETL transformations to {source_name} data...")
        # Step 1: Base Transform (MA, ZScore, Volatility, Trend, Suggestion)
        transformed_df = self.transformer.transform(raw_df.copy())
        
        # Step 2: Load to Databases (using threads for non-blocking UI)
        rows_for_db = transformed_df.values.tolist()
        
        # [cite_start]Load into SQLite (Local) [cite: 38]
        threading.Thread(target=self.db.insert_rows, args=(rows_for_db, f"local_{source_name.lower()}")).start()
        
        # [cite_start]Load into MySQL (Optional, remote) [cite: 39]
        if self.mysql_db.conn:
             # This is run in a thread to prevent blocking the GUI for external DB latency
             threading.Thread(target=self.mysql_db.insert_rows, args=(rows_for_db, f"remote_{source_name.lower()}")).start()
        
        self.populate_treeview(tree_transformed, transformed_df)
        self.log(f"✅ {source_name} transformed and loaded to DBs. Records: {len(transformed_df)}")
        return transformed_df
        
    def apply_yahoo_base_transform(self):
        self.yahoo_transformed = self._apply_transform_and_load(self.yahoo_data, self.tree_yahoo_transformed, "Yahoo")

    def apply_csv_base_transform(self):
        self.csv_transformed = self._apply_transform_and_load(self.csv_data, self.tree_csv_transformed, "CSV")
        
    # Extra Transformations
    def _apply_extra_transform(self, df_attr, method_name, tree, label):
        current_df = self.tree_to_df(tree)
        if current_df.empty:
            self.log(f"⚠️ Cannot apply {label}: Transformed data is empty.", level='warning')
            return

        before_df = current_df.copy()
        
        # Apply the specific extra transformation
        if method_name == 'daily_return':
            after_df = self.transformer.add_daily_return(current_df)
        elif method_name == 'fill_missing':
            after_df = self.transformer.fill_missing_values(current_df)
        elif method_name == 'normalize_price':
            after_df = self.transformer.normalize_price(current_df)
        else:
            return

        # Update the Treeview and internal DataFrame attribute
        self.populate_treeview(tree, after_df)
        setattr(self, df_attr, after_df)
        
        self.show_transform_changes(before_df, after_df, label)


    def apply_yahoo_daily_return(self):
        self._apply_extra_transform("yahoo_transformed", 'daily_return', self.tree_yahoo_transformed, "Yahoo Daily Return")
    def apply_yahoo_fill_missing(self):
        self._apply_extra_transform("yahoo_transformed", 'fill_missing', self.tree_yahoo_transformed, "Yahoo Fill Missing")
    def apply_yahoo_normalize(self):
        self._apply_extra_transform("yahoo_transformed", 'normalize_price', self.tree_yahoo_transformed, "Yahoo Normalize Price")

    def apply_csv_daily_return(self):
        self._apply_extra_transform("csv_transformed", 'daily_return', self.tree_csv_transformed, "CSV Daily Return")
    def apply_csv_fill_missing(self):
        self._apply_extra_transform("csv_transformed", 'fill_missing', self.tree_csv_transformed, "CSV Fill Missing")
    def apply_csv_normalize(self):
        self._apply_extra_transform("csv_transformed", 'normalize_price', self.tree_csv_transformed, "CSV Normalize Price")


    def save_csv(self):
        # Logic to decide which DataFrame to save
        df_to_save = None
        source_name = "data"
        
        if not self.yahoo_transformed.empty:
            df_to_save = self.yahoo_transformed
            source_name = "yahoo_transformed"
        elif not self.csv_transformed.empty:
            df_to_save = self.csv_transformed
            source_name = "csv_transformed"
        elif not self.yahoo_data.empty:
            df_to_save = self.yahoo_data
            source_name = "yahoo_raw"

        if df_to_save is None or df_to_save.empty:
            messagebox.showwarning("Save Warning", "No data available to save.")
            return

        filename = filedialog.asksaveasfilename(defaultextension=".csv",
                                                initialfile=f"stock_data_{source_name}_{datetime.date.today()}.csv",
                                                filetypes=[("CSV files", "*.csv")])
        if filename:
            try:
                df_to_save.to_csv(filename, index=False)
                self.log(f"✅ Data saved successfully to: {os.path.basename(filename)}")
            except Exception as e:
                messagebox.showerror("Save Error", f"Failed to save file: {e}")
                self.log(f"❌ Error saving file: {e}", level='error')

    # ----- Auto-Refresh Logic -----
    def toggle_auto_refresh(self):
        if self.auto_refresh_var.get():
            try:
                interval = int(self.refresh_interval_spin.get()) * 1000
                if interval <= 0:
                    raise ValueError
                self.log(f"🟢 Auto-refresh enabled. Interval: {interval//1000} seconds.")
                self.start_auto_refresh(interval)
            except ValueError:
                self.log("❌ Invalid refresh interval. Please enter a number > 0.", level='error')
                self.auto_refresh_var.set(False)
        else:
            self.stop_auto_refresh()
            self.log("🔴 Auto-refresh disabled.")

    def start_auto_refresh(self, interval):
        # Stop any existing job first
        if self.refresh_job:
            self.root.after_cancel(self.refresh_job)
        
        # Function to run
        def refresh_task():
            if self.auto_refresh_var.get():
                self.fetch_yahoo() # Reruns the entire Yahoo ETL process
                self.refresh_job = self.root.after(interval, refresh_task) # Schedule next run

        self.fetch_yahoo() # Run immediately once
        self.refresh_job = self.root.after(interval, refresh_task) # Schedule first interval
        
    def stop_auto_refresh(self):
        if self.refresh_job:
            self.root.after_cancel(self.refresh_job)
            self.refresh_job = None
            
    # ----- OLAP Methods -----
    def _run_query(self, db_interface, db_name):
        sql = self.query_entry.get().strip()
        if not sql:
            messagebox.showwarning("Query Warning", "SQL query cannot be empty.")
            return

        self.log(f"🟡 Running query on {db_name}...")
        
        # [cite_start]This demonstrates analytical querying (OLAP) for decision-making [cite: 14]
        df_result = db_interface.query(sql)
        
        self.query_tree.delete(*self.query_tree.get_children())
        
        if df_result.empty:
            self.query_tree["columns"] = []
            self.log(f"✅ Query executed on {db_name}, but returned no results.", level='warning')
            return

        cols = list(df_result.columns)
        self.query_tree["columns"] = cols
        
        for col in cols:
            self.query_tree.heading(col, text=col)
            width = max(len(col) * 12, 120)
            self.query_tree.column(col, width=width, anchor="w")

        # Insert data
        for _, row in df_result.iterrows():
            values = [f"{val:.4f}" if isinstance(val, (float, int)) else str(val) for val in row.values]
            self.query_tree.insert("", "end", values=values)
            
        self.log(f"✅ Query executed successfully on {db_name}. Results: {len(df_result)} rows.")


    def run_query_sqlite(self):
        self._run_query(self.db, "SQLite (Local)")

    def run_query_mysql(self):
        if not self.mysql_db.conn:
            messagebox.showwarning("MySQL Warning", "MySQL connection is not established.")
            return
        self._run_query(self.mysql_db, "MySQL (Remote)")


# ===================== Main Execution =====================
if __name__ == "__main__":
    root = tk.Tk()
    app = ETLApp(root)
    root.mainloop()