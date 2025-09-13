import sys
import argparse
import pandas as pd
from datetime import datetime, date
from typing import List
import os

# 로컬 모듈 import (동일 디렉터리의 dividend_export.py 사용)
try:
    import dividend_export as de
except Exception as e:
    print("dividend_export 모듈 임포트 실패:", e)
    sys.exit(1)

# -----------------------------------------------------------
# 날짜 파싱: YYYYMMDD 형식의 문자열을 date 객체로 변환.
# -----------------------------------------------------------
def parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()

# -----------------------------------------------------------
# 종목 마켓 코드 변환: krx_export.csv의 MARKET 값을 dividend_export의 market 코드로 변환.
#    - KOSPI -> KS
#    - KOSDAQ -> KQ
#    - ETF -> KS (대부분 KOSPI 상장 ETF)
#    기타는 공백 반환
# -----------------------------------------------------------
def market_to_de_market(market: str) -> str:
    m = (market or "").upper()
    if m == "KOSPI":
        return "KS"
    if m == "KOSDAQ":
        return "KQ"
    if m == "ETF":
        return "KS"
    return ""

# -----------------------------------------------------------
# 배당 조회: 하나의 종목(ISCD)에 대해 배당 DF를 반환.
#   dividend_export.fetch_dividends_yf 사용.
# -----------------------------------------------------------
def fetch_dividends_for_row(iscd: str, market: str, start: date, end: date) -> pd.DataFrame:
    mapped = market_to_de_market(market)
    try:
        df = de.fetch_dividends_yf(iscd, mapped, start, end)
        if df is None or df.empty:
            return pd.DataFrame(columns=["ISCD", "기준일", "배당금"])
        return df
    except Exception:
        return pd.DataFrame(columns=["ISCD", "기준일", "배당금"])


# -----------------------------------------------------------
# 메인: CSV 일괄 처리
# -----------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="배치: kr_stocks_etfs.csv 기반 배당 정보 일괄 수집")
    ap.add_argument("--in", dest="infile", default="kr_stocks_etfs.csv", help="입력 CSV (krx_export 출력)")
    ap.add_argument("--out", dest="outfile", default="kr_dividends_20200101_20250828.csv", help="출력 CSV 파일")
    ap.add_argument("--start", default="20200101", help="시작일 YYYYMMDD (예: 20200101)")
    ap.add_argument("--end", default="20250828", help="종료일 YYYYMMDD (예: 20250828)")
    ap.add_argument("--only", default=None, help="특정 종목코드(콤마 구분)만 처리 (옵션)")
    ap.add_argument("--row-start", type=int, default=None, help="처리 시작 행 (1-base, 포함)")
    ap.add_argument("--row-end", type=int, default=None, help="처리 종료 행 (1-base, 포함)")
    args = ap.parse_args()

    # 날짜 파싱
    try:
        start_d = parse_ymd(args.start)
        end_d = parse_ymd(args.end)
    except Exception:
        sys.exit("날짜 형식이 잘못되었습니다. YYYYMMDD")

    # 입력 CSV 로드
    try:
        src = pd.read_csv(args.infile, dtype={"ISCD": str, "KR_ISNM": str, "MARKET": str})
    except Exception as e:
        sys.exit(f"입력 CSV 로드 실패: {e}")

    # 필터: only
    if args.only:
        only_set = {c.strip() for c in args.only.split(',') if c.strip()}
        src = src[src["ISCD"].isin(only_set)].reset_index(drop=True)


    # 행 범위 슬라이싱 (1-base inclusive)
    total_all = len(src)
    start_offset = 0  # 원본 기준 시작 오프셋(0-base)
    if args.row_start is not None or args.row_end is not None:
        if args.row_start is None or args.row_end is None:
            sys.exit("--row-start와 --row-end는 함께 지정해야 합니다.")
        if args.row_start <= 0 or args.row_end <= 0:
            sys.exit("행 번호는 1 이상의 정수여야 합니다.")
        if args.row_start > args.row_end:
            sys.exit("row 범위가 올바르지 않습니다: row-start <= row-end 이어야 합니다.")
        start_idx = args.row_start - 1
        end_idx = args.row_end - 1
        if start_idx >= total_all:
            sys.exit(f"row-start가 데이터 범위를 벗어났습니다. 총 행수: {total_all}")
        end_idx = min(end_idx, total_all - 1)
        start_offset = start_idx
        src = src.iloc[start_idx:end_idx+1].reset_index(drop=True)
        print(f"행 범위 적용: {args.row_start}~{args.row_end} (원본 {total_all}행 중 {len(src)}행 처리)")

    results: List[pd.DataFrame] = []
    total = len(src)
    print(f"총 {total}개 종목 처리 시작...")

    for idx, row in src.iterrows():
        iscd = str(row.get("ISCD", "")).zfill(6)
        name = row.get("KR_ISNM", "")
        market = row.get("MARKET", "")
        absolute_row_num = start_offset + idx + 1  # 원본 기준 1-base
        print(f"[{absolute_row_num}/{total_all}] (chunk {idx+1}/{total}) {iscd} {name} ({market}) 배당 조회 중...", end="")

        df = fetch_dividends_for_row(iscd, market, start_d, end_d)
        fetched_count = 0 if df is None or df.empty else len(df)
        print(f"    -> 수집 건수: {fetched_count}")
        if df is None or df.empty:
            continue
        # 메타 정보 부가
        df["KR_ISNM"] = name
        df["MARKET"] = market
        results.append(df)

    if not results:
        print("수집된 배당 데이터가 없습니다.")
        # 빈 파일이라도 생성 또는 기존 파일 유지
        if not os.path.exists(args.outfile):
            pd.DataFrame(columns=["ISCD","기준일","배당금","KR_ISNM","MARKET"]).to_csv(
                args.outfile, index=False, encoding="utf-8-sig"
            )
            print(f"빈 결과를 '{args.outfile}'로 저장했습니다.")
        return

    out_df = pd.concat(results, ignore_index=True)
    # 정렬: ISCD, 기준일
    try:
        # 기준일이 YYYY.M.D 형태이므로 파싱 후 정렬
        def parse_dot(s: str) -> date:
            y, m, d = s.split('.')
            return date(int(y), int(m), int(d))
        out_df['__dt'] = out_df['기준일'].apply(parse_dot)
        out_df = out_df.sort_values(["ISCD", "__dt"]).drop(columns='__dt')
    except Exception:
        out_df = out_df.sort_values(["ISCD", "기준일"])

    # 컬럼 순서 고정
    cols = ["ISCD","기준일","배당금","KR_ISNM","MARKET"]
    out_df = out_df[cols]

    # 이어쓰기(append) 지원: 기존 파일이 있으면 헤더 없이 추가, 없으면 생성
    file_exists = os.path.exists(args.outfile)
    mode = 'a' if file_exists else 'w'
    header = not file_exists
    out_df.to_csv(args.outfile, index=False, encoding="utf-8-sig", mode=mode, header=header)

    action = "추가" if file_exists else "저장"
    print(f"완료: {len(out_df)}행을 '{args.outfile}'에 {action}했습니다.")

#python3 batch_dividends.py --row-start 2001 --row-end 3782 --out kr_divs.csv
if __name__ == "__main__":
    main()
