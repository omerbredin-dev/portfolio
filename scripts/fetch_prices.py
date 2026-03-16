#!/usr/bin/env python3
"""
Fetches all portfolio prices server-side (no CORS issues).
Writes prices.json for the portfolio page to consume.

Sources:
  - TradingView Scanner API for TASE (Israeli) holdings + market indexes
  - Yahoo Finance v8 chart API for US/EU holdings
  - open.er-api.com for USD/ILS exchange rate
"""

import json, urllib.request, urllib.error, time, sys
from datetime import datetime, timezone

# ── Configuration ──

TASE_SYMBOLS = {
    'BEZQ.TA':    'TASE:BEZQ',
    'KSMF74.TA':  'TASE:KSM.F74',
    'KSMF57.TA':  'TASE:KSM.F57',
    'HRLF15.TA':  'TASE:HRL.F15',
    'TCHF12.TA':  'TASE:TCH.F12',
    'iSFF301.TA': 'TASE:IS.FF301',
    'iSFF501.TA': 'TASE:IS.FF501',
    'INFF1.TA':   'TASE:IN.FF1',
    'iSFF702.TA': 'TASE:IS.FF702',
}
TV_REV = {v: k for k, v in TASE_SYMBOLS.items()}

GLOBAL_SYMBOLS = [
    'NVDA', 'ASML', 'AAPL', 'MSFT', 'AMZN', 'INTC', 'GOOGL',
    'NET', 'MA', 'ACN', 'DAL', 'BA', 'SEDG', 'MDB', 'TWLO',
    'GLBE', 'ESTC', 'QQQM', 'VOO', 'IBIT', 'TAN', 'QTUM',
]
# BRK-B needs special Yahoo ticker
YAHOO_TICKER = {'BRKb': 'BRK-B'}
GLOBAL_SYMBOLS.append('BRKb')

INDEX_TICKERS = {
    'SP:SPX':          {'name': 'S&P 500',  'currency': 'USD'},
    'NASDAQ:NDX':      {'name': 'Nasdaq',   'currency': 'USD'},
    'XETR:DAX':        {'name': 'DAX',      'currency': 'EUR'},
    'TVC:SX5E':        {'name': 'STOXX 50', 'currency': 'EUR'},
    'TASE:TA125':      {'name': 'TA-125',   'currency': 'ILS'},
    'BITSTAMP:BTCUSD': {'name': 'Bitcoin',  'currency': 'USD'},
}


def fetch_json(url, data=None, timeout=15):
    """Fetch URL, optionally POST JSON data."""
    headers = {'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0'}
    if data:
        req = urllib.request.Request(url, data=json.dumps(data).encode(), headers=headers, method='POST')
    else:
        req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def fetch_tase_prices():
    """Fetch TASE prices via TradingView scanner."""
    tickers = list(TASE_SYMBOLS.values())
    body = {'symbols': {'tickers': tickers}, 'columns': ['close', 'change']}
    result = fetch_json('https://scanner.tradingview.com/israel/scan', data=body)
    out = {}
    for item in result.get('data', []):
        tv_sym = item['s']
        port_sym = TV_REV.get(tv_sym)
        if port_sym and item['d'][0] is not None:
            out[port_sym] = {'price': item['d'][0] / 100, 'dailyPct': item['d'][1] or 0}
    return out


def fetch_global_prices():
    """Fetch US/EU stock prices via Yahoo Finance v8 chart API."""
    out = {}
    for sym in GLOBAL_SYMBOLS:
        try:
            ticker = YAHOO_TICKER.get(sym, sym)
            url = f'https://query2.finance.yahoo.com/v8/finance/chart/{urllib.request.quote(ticker)}?interval=1d&range=5d'
            data = fetch_json(url)
            result = data.get('chart', {}).get('result', [{}])[0]
            closes = (result.get('indicators', {}).get('adjclose', [{}])[0].get('adjclose')
                     or result.get('indicators', {}).get('quote', [{}])[0].get('close', []))
            valid = [c for c in closes if c is not None]
            if valid:
                last = valid[-1]
                prev = valid[-2] if len(valid) >= 2 else None
                daily_pct = ((last - prev) / prev * 100) if prev else 0
                out[sym] = {'price': last, 'dailyPct': round(daily_pct, 4)}
        except Exception as e:
            print(f'  WARN: {sym} failed: {e}', file=sys.stderr)
    return out


def fetch_indexes():
    """Fetch market indexes via TradingView scanner (global exchange)."""
    tickers = list(INDEX_TICKERS.keys())
    body = {'symbols': {'tickers': tickers}, 'columns': ['close', 'change']}
    result = fetch_json('https://scanner.tradingview.com/global/scan', data=body)
    out = {}
    for item in result.get('data', []):
        tv = item['s']
        info = INDEX_TICKERS.get(tv)
        if info and item['d'][0] is not None:
            out[tv] = {'price': item['d'][0], 'chgPct': round(item['d'][1] or 0, 4), **info}
    return out


def fetch_fx_rate():
    """Fetch USD/ILS rate from open.er-api.com."""
    try:
        data = fetch_json('https://open.er-api.com/v6/latest/USD')
        return data.get('rates', {}).get('ILS')
    except:
        return None


def main():
    print('Fetching prices...', file=sys.stderr)

    prices = {}
    errors = []

    # Fetch TASE
    try:
        tase = fetch_tase_prices()
        prices.update(tase)
        print(f'  TASE: {len(tase)}/{len(TASE_SYMBOLS)} symbols', file=sys.stderr)
    except Exception as e:
        errors.append(f'TASE: {e}')
        print(f'  TASE ERROR: {e}', file=sys.stderr)

    # Fetch Global
    try:
        glob = fetch_global_prices()
        prices.update(glob)
        print(f'  Global: {len(glob)}/{len(GLOBAL_SYMBOLS)} symbols', file=sys.stderr)
    except Exception as e:
        errors.append(f'Global: {e}')
        print(f'  Global ERROR: {e}', file=sys.stderr)

    # Fetch Indexes
    indexes = {}
    try:
        indexes = fetch_indexes()
        print(f'  Indexes: {len(indexes)}/{len(INDEX_TICKERS)}', file=sys.stderr)
    except Exception as e:
        errors.append(f'Indexes: {e}')
        print(f'  Indexes ERROR: {e}', file=sys.stderr)

    # Fetch FX rate
    fx = fetch_fx_rate()
    if fx:
        print(f'  FX USD/ILS: {fx}', file=sys.stderr)
    else:
        errors.append('FX rate unavailable')
        print('  FX ERROR: unavailable', file=sys.stderr)

    # Write output
    output = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'prices': prices,
        'indexes': indexes,
        'fx': {'USDILS': fx} if fx else {},
        'errors': errors,
    }

    out_path = 'prices.json'
    with open(out_path, 'w') as f:
        json.dump(output, f, indent=1)

    total = len(prices)
    expected = len(TASE_SYMBOLS) + len(GLOBAL_SYMBOLS)
    print(f'Done: {total}/{expected} prices, {len(indexes)} indexes, FX={fx}', file=sys.stderr)
    print(f'Written to {out_path}', file=sys.stderr)

    # Exit with error if too many symbols missing
    if total < expected * 0.5:
        print('ERROR: Too many missing prices!', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
