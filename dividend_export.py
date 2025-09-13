"""
pykrx 기반 배당 수집 스크립트 -> 실패 
-> 네이버 금융 스크래핑으로 변경 -> 실패
-> yfinance 기반 배당 수집 스크립트로 변경  -> 성공
(OPEN DART 방법도 있음 참고)
yfinance 기반 배당 수집 스크립트 (한국 + 미국 지원)
- 한국: .KS, .KQ 접미사 붙여서 조회
- 미국: 접미사 없음 (AAPL, MSFT 등)
"""

import argparse
from datetime import datetime, date
import pandas as pd

try:
    import yfinance as yf
except Exception as e:
    raise ImportError("yfinance가 필요합니다. 설치: pip install yfinance") from e

DATEFMT_IN = "%Y%m%d"

def parse_args():
    p = argparse.ArgumentParser(description="yfinance로 배당(기준일/금액) 조회 후 CSV 저장")
    p.add_argument("--ticker", required=True, help="종목코드 또는 티커 (예: 001720, AAPL)")
    p.add_argument("--market", required=True, 
                   choices=["KOSPI","KOSDAQ","KONEX","KS","KQ","US"], 
                   help="시장 코드 (KOSPI/KOSDAQ/KONEX/US)")
    p.add_argument("--start", required=True, help="시작일 YYYYMMDD")
    p.add_argument("--end", required=True, help="종료일 YYYYMMDD")
    p.add_argument("--out", required=True, help="출력 CSV 파일명")
    return p.parse_args()

def to_suffix(market: str) -> str:
    """시장 코드에 따라 Yahoo 티커 접미사 반환"""
    m = market.upper()
    if m in ("KOSPI", "KS"): return "KS"
    if m in ("KOSDAQ", "KQ"): return "KQ"
    if m in ("US",): return ""  # 미국장은 접미사 없음
    return ""

def parse_ymd(s: str) -> date:
    return datetime.strptime(s, DATEFMT_IN).date()

def fmt_ymd_dot(d: date) -> str:
    return f"{d.year}.{d.month}.{d.day}"

def fetch_dividends_yf(ticker: str, market: str, start: date, end: date) -> pd.DataFrame:
    suffix = to_suffix(market)
    # 한국 종목코드는 6자리로 zfill 필요, 미국은 그대로 사용
    if market.upper() in ("US",):
        yf_ticker = ticker
        iscd = ticker.upper()  # 미국 티커는 대문자로 저장
    else:
        yf_ticker = f"{ticker.zfill(6)}.{suffix}" if suffix else ticker.zfill(6)
        iscd = ticker.zfill(6)

    tk = yf.Ticker(yf_ticker)
    divs = tk.dividends
    if divs is None or divs.empty:
        return pd.DataFrame(columns=["ISCD","기준일","배당금"])

    try:
        mask = (divs.index.date >= start) & (divs.index.date <= end)
        sel = divs.loc[mask]
    except Exception:
        rows = []
        for idx, val in divs.items():
            d = idx.date()
            if start <= d <= end:
                rows.append((d, val))
        if not rows:
            return pd.DataFrame(columns=["ISCD","기준일","배당금"])
        sel = pd.Series({pd.Timestamp(r[0]): r[1] for r in rows})

    out_rows = []
    for idx, amt in sel.items():
        d = idx.date()
        if pd.isna(amt):
            continue
        try:
            f = float(amt)
            amt_val = int(f) if f.is_integer() else f
        except Exception:
            amt_val = amt
        out_rows.append({"ISCD": iscd, "기준일": fmt_ymd_dot(d), "배당금": amt_val})

    return pd.DataFrame(out_rows)

def main():
    args = parse_args()
    start = parse_ymd(args.start)
    end = parse_ymd(args.end)

    df = fetch_dividends_yf(args.ticker, args.market, start, end)

    if not df.empty:
        def parse_dot(s):
            y,m,d = s.split('.')
            return date(int(y), int(m), int(d))
        df['__dt'] = df['기준일'].apply(parse_dot)
        df = df.sort_values('__dt').drop(columns='__dt')

    df.to_csv(args.out, index=False, encoding='utf-8-sig')
    print(f"Wrote {len(df)} rows to {args.out}")

#python3 dividend_export.py --ticker TSLY --market US --start 20240101 --end 20250801 --out TSLY_dividends.csv
if __name__ == '__main__':
    main()