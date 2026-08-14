"""
Gold Layer REST API for BI integrations.
Data source: Trino -> Delta Gold tables.
"""

import os
import re
from datetime import date, datetime
from decimal import Decimal

from flask import Flask, jsonify, request
from flask_cors import CORS
from trino import dbapi
from trino.exceptions import DatabaseError, TrinoQueryError

app = Flask(__name__)
CORS(app)

TABLES = {
    "ohlc": "delta.gold.ohlc_1min",
    "whale": "delta.gold.whale_alert",
    "flow": "delta.gold.maker_taker_flow_1min",
    "vwap": "delta.gold.vwap_1min",
}

SYMBOL_PATTERN = re.compile(r"^[A-Z0-9_]{1,20}$")


def _serialize(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _rows_to_dicts(cursor):
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    return [
        {key: _serialize(value) for key, value in zip(columns, row)}
        for row in rows
    ]


def _get_conn():
    return dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "admin"),
        catalog=os.getenv("TRINO_CATALOG", "delta"),
        schema=os.getenv("TRINO_SCHEMA", "gold"),
        http_scheme=os.getenv("TRINO_HTTP_SCHEME", "http"),
    )


def _run_query(sql):
    conn = _get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        return _rows_to_dicts(cursor)
    finally:
        cursor.close()
        conn.close()


def _int_query_param(name, default_value, min_value=1, max_value=1000):
    raw = request.args.get(name, str(default_value))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"'{name}' must be an integer.") from exc
    if value < min_value or value > max_value:
        raise ValueError(f"'{name}' must be between {min_value} and {max_value}.")
    return value


def _float_query_param(name, default_value, min_value=0):
    raw = request.args.get(name, str(default_value))
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"'{name}' must be a number.") from exc
    if value < min_value:
        raise ValueError(f"'{name}' must be >= {min_value}.")
    return value


def _symbol_query_param():
    symbol = request.args.get("symbol")
    if symbol is None or symbol.strip() == "":
        return None
    normalized = symbol.strip().upper()
    if not SYMBOL_PATTERN.match(normalized):
        raise ValueError("'symbol' must match [A-Z0-9_], max 20 chars.")
    return normalized


def _quote_string(value):
    return "'" + value.replace("'", "''") + "'"


@app.errorhandler(ValueError)
def handle_value_error(err):
    return jsonify({"success": False, "error": str(err)}), 400


@app.errorhandler(TrinoQueryError)
def handle_trino_error(err):
    return jsonify(
        {
            "success": False,
            "error": err.message,
            "error_name": err.error_name,
            "query_id": err.query_id,
        }
    ), 500


@app.errorhandler(DatabaseError)
def handle_trino_database_error(err):
    return jsonify({"success": False, "error": str(err)}), 500


@app.route("/")
def index():
    return jsonify(
        {
            "status": "running",
            "service": "Crypto Lakehouse Gold API (Trino)",
            "timestamp": datetime.now().isoformat(),
            "data_source": {
                "engine": "trino",
                "host": os.getenv("TRINO_HOST", "trino"),
                "port": int(os.getenv("TRINO_PORT", "8080")),
                "catalog": os.getenv("TRINO_CATALOG", "delta"),
                "schema": os.getenv("TRINO_SCHEMA", "gold"),
            },
            "endpoints": [
                "/api/health",
                "/api/ohlc/latest",
                "/api/whale/latest",
                "/api/flow/latest",
                "/api/vwap/latest",
                "/api/dashboard/summary",
                "/api/stats",
            ],
        }
    )


@app.route("/api/health")
def health():
    data = _run_query("SELECT 1 AS ok")
    return jsonify({"success": True, "data": data, "timestamp": datetime.now().isoformat()})


@app.route("/api/ohlc/latest")
def get_ohlc_latest():
    limit = _int_query_param("limit", 100, min_value=1, max_value=5000)
    symbol = _symbol_query_param()

    where_clause = ""
    if symbol:
        where_clause = f"WHERE symbol = {_quote_string(symbol)}"

    query = f"""
        SELECT symbol, candle_time, open_price, high_price, low_price, close_price,
               total_quantity, total_quote_qty, total_trades
        FROM {TABLES["ohlc"]}
        {where_clause}
        ORDER BY candle_time DESC
        LIMIT {limit}
    """
    data = _run_query(query)
    return jsonify({"success": True, "count": len(data), "data": data, "timestamp": datetime.now().isoformat()})


@app.route("/api/whale/latest")
def get_whale_latest():
    limit = _int_query_param("limit", 50, min_value=1, max_value=2000)
    min_value = _float_query_param("min_value", 50000, min_value=0)
    symbol = _symbol_query_param()

    where_clauses = [f"trade_value_usdt >= {min_value}"]
    if symbol:
        where_clauses.append(f"symbol = {_quote_string(symbol)}")

    where_clause = "WHERE " + " AND ".join(where_clauses)

    query = f"""
        SELECT event_id, symbol, event_time, price, quantity, quote_qty,
               is_buyer_maker, trade_value_usdt
        FROM {TABLES["whale"]}
        {where_clause}
        ORDER BY event_time DESC
        LIMIT {limit}
    """
    data = _run_query(query)
    return jsonify(
        {
            "success": True,
            "count": len(data),
            "min_value_filter": min_value,
            "data": data,
            "timestamp": datetime.now().isoformat(),
        }
    )


@app.route("/api/flow/latest")
def get_flow_latest():
    limit = _int_query_param("limit", 100, min_value=1, max_value=5000)
    symbol = _symbol_query_param()

    where_clause = ""
    if symbol:
        where_clause = f"WHERE symbol = {_quote_string(symbol)}"

    query = f"""
        SELECT symbol, window_start, buy_aggressive_qty, sell_aggressive_qty,
               buy_aggressive_quote_qty, sell_aggressive_quote_qty, net_flow
        FROM {TABLES["flow"]}
        {where_clause}
        ORDER BY window_start DESC
        LIMIT {limit}
    """
    data = _run_query(query)
    return jsonify({"success": True, "count": len(data), "data": data, "timestamp": datetime.now().isoformat()})


@app.route("/api/vwap/latest")
def get_vwap_latest():
    limit = _int_query_param("limit", 100, min_value=1, max_value=5000)
    symbol = _symbol_query_param()

    where_clause = ""
    if symbol:
        where_clause = f"WHERE symbol = {_quote_string(symbol)}"

    query = f"""
        SELECT symbol, window_start, total_quantity, total_quote_qty, trade_count,
               close_price, close_event_time, vwap_price, avg_trade_size,
               close_vs_vwap_diff, close_vs_vwap_pct
        FROM {TABLES["vwap"]}
        {where_clause}
        ORDER BY window_start DESC
        LIMIT {limit}
    """
    data = _run_query(query)
    return jsonify({"success": True, "count": len(data), "data": data, "timestamp": datetime.now().isoformat()})


@app.route("/api/dashboard/summary")
def get_dashboard_summary():
    symbol = _symbol_query_param()

    symbol_where = ""
    if symbol:
        symbol_where = f"WHERE symbol = {_quote_string(symbol)}"

    recent_ohlc = _run_query(
        f"""
        SELECT symbol, close_price, candle_time
        FROM {TABLES["ohlc"]}
        {symbol_where}
        ORDER BY candle_time DESC
        LIMIT 60
    """
    )
    recent_flow = _run_query(
        f"""
        SELECT net_flow, window_start
        FROM {TABLES["flow"]}
        {symbol_where}
        ORDER BY window_start DESC
        LIMIT 60
    """
    )

    whale_where_clauses = [
        "trade_value_usdt >= 50000",
        "event_time >= current_timestamp - INTERVAL '1' HOUR",
    ]
    if symbol:
        whale_where_clauses.append(f"symbol = {_quote_string(symbol)}")

    whale_agg = _run_query(
        f"""
        SELECT COUNT(*) AS whale_count_1h,
               COALESCE(SUM(trade_value_usdt), 0) AS whale_total_value_1h
        FROM {TABLES["whale"]}
        WHERE {' AND '.join(whale_where_clauses)}
    """
    )

    latest_vwap = _run_query(
        f"""
        SELECT close_vs_vwap_pct, window_start
        FROM {TABLES["vwap"]}
        {symbol_where}
        ORDER BY window_start DESC
        LIMIT 1
    """
    )

    latest_price_row = recent_ohlc[0] if recent_ohlc else {}
    base_price_row = recent_ohlc[-1] if recent_ohlc else {}
    whale_row = whale_agg[0] if whale_agg else {}
    vwap_row = latest_vwap[0] if latest_vwap else {}

    latest_close = float(latest_price_row.get("close_price", 0) or 0)
    base_close = float(base_price_row.get("close_price", 0) or 0)

    price_change_1h_pct = 0.0
    if base_close != 0:
        price_change_1h_pct = ((latest_close - base_close) / base_close) * 100

    net_flow_1h = sum(float(row.get("net_flow", 0) or 0) for row in recent_flow)
    whale_count_1h = int(whale_row.get("whale_count_1h", 0) or 0)
    whale_total_value_1h = float(whale_row.get("whale_total_value_1h", 0) or 0)
    latest_close_vs_vwap_pct = float(vwap_row.get("close_vs_vwap_pct", 0) or 0)

    summary = {
        "latest_price": {
            "symbol": latest_price_row.get("symbol") or symbol,
            "price": latest_close if latest_price_row.get("close_price") is not None else None,
            "time": latest_price_row.get("candle_time"),
        },
        "price_change_1h_pct": price_change_1h_pct,
        "net_flow_1h": net_flow_1h,
        "whale_count_1h": whale_count_1h,
        "whale_total_value_1h": whale_total_value_1h,
        "latest_close_vs_vwap_pct": latest_close_vs_vwap_pct,
        "market_sentiment": "BULLISH" if net_flow_1h > 0 else ("BEARISH" if net_flow_1h < 0 else "NEUTRAL"),
        "window_note": "Metrics are computed from latest up to 60 one-minute rows.",
        "whale_count_recent": whale_count_1h,
        "whale_total_value": whale_total_value_1h,
        "net_flow": net_flow_1h,
    }
    return jsonify({"success": True, "summary": summary, "timestamp": datetime.now().isoformat()})


@app.route("/api/stats")
def get_stats():
    stats = {}
    for name, table in TABLES.items():
        row = _run_query(f"SELECT COUNT(*) AS total_records FROM {table}")
        total_records = int(row[0]["total_records"]) if row else 0
        stats[name] = {"total_records": total_records, "table": table}

    return jsonify({"success": True, "stats": stats, "timestamp": datetime.now().isoformat()})


if __name__ == "__main__":
    port = int(os.getenv("API_PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"

    print(
        f"""
    ╔══════════════════════════════════════════════════════════════════╗
    ║  Crypto Lakehouse Gold API (Trino-backed)                       ║
    ║  Running on http://0.0.0.0:{port:<5}                                 ║
    ╠══════════════════════════════════════════════════════════════════╣
    ║  GET /api/health                                                ║
    ║  GET /api/ohlc/latest                                           ║
    ║  GET /api/whale/latest                                          ║
    ║  GET /api/flow/latest                                           ║
    ║  GET /api/vwap/latest                                           ║
    ║  GET /api/dashboard/summary                                     ║
    ║  GET /api/stats                                                 ║
    ╚══════════════════════════════════════════════════════════════════╝
    """
    )

    app.run(host="0.0.0.0", port=port, debug=debug)
