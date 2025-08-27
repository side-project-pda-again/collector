import sys
import argparse
import pandas as pd
from pykrx import stock
import requests

# -----------------------------------------------------------
# pykrx 라이브러리를 이용해 KOSPI/KOSDAQ/ETF 종목 정보를 수집하는 함수
# -----------------------------------------------------------
def fetch_with_pykrx(markets):
    rows = []

    # KOSPI, KOSDAQ 종목 가져오기
    if "KOSPI" in markets or "KOSDAQ" in markets:
        for m in ("KOSPI", "KOSDAQ"):
            if m not in markets:
                continue
            # 각 시장의 종목 코드 리스트 조회
            for tk in stock.get_market_ticker_list(market=m):
                rows.append({
                    "ISCD": str(tk).zfill(6),   # 종목 코드 (6자리 0채움)
                    "KR_ISNM": stock.get_market_ticker_name(tk),  # 종목명
                    "MARKET": m  # 시장 구분
                })

    # ETF 종목 가져오기
    if "ETF" in markets:
        for tk in stock.get_etf_ticker_list():
            try:
                name = stock.get_etf_ticker_name(tk)
            except Exception:
                name = ""
            rows.append({
                "ISCD": str(tk),   # ETF 종목 코드
                "KR_ISNM": name,   # ETF 이름
                "MARKET": "ETF"
            })

    # DataFrame 변환 및 중복 제거/정렬
    df = pd.DataFrame(rows, columns=["ISCD", "KR_ISNM", "MARKET"])
    if df.empty:
        raise RuntimeError("pykrx에서 데이터를 받지 못했습니다.")
    return (df.drop_duplicates(subset=["ISCD"])  # 종목코드 기준 중복 제거
              .sort_values(["MARKET", "ISCD"])   # 시장, 코드 순 정렬
              .reset_index(drop=True))


# -----------------------------------------------------------
# 메인 함수: 실행 시 CLI 인자를 받아 CSV 저장
# -----------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="KRX 상장 종목 CSV 저장기")
    # 수집할 시장 (기본값: KOSPI,KOSDAQ,ETF)
    ap.add_argument("--markets", default="KOSPI,KOSDAQ,ETF",
                    help="대상 시장: KOSPI,KOSDAQ,ETF (콤마로 구분)")
    # 출력 파일명 (기본값: kr_stocks_etfs.csv)
    ap.add_argument("--out", default="kr_stocks_etfs.csv",
                    help="출력 파일명")
    args = ap.parse_args()

    # 인자로 받은 시장을 set으로 정리
    markets = {m.strip().upper() for m in args.markets.split(",") if m.strip()}
    allowed = {"KOSPI", "KOSDAQ", "ETF"}
    # 허용되지 않은 시장 입력 시 종료
    if markets - allowed:
        sys.exit(f"지원하지 않는 시장: {markets - allowed}")

    try:
        print("pykrx로 수집 중...")
        df = fetch_with_pykrx(markets)  # pykrx 방식으로 데이터 수집
    except Exception as e:
        # pykrx 실패 시 웹 다운로드 방식으로 대체
        print("pykrx 실패:", e)

    # 결과 CSV 저장 (UTF-8-SIG 인코딩: Excel에서 한글 깨짐 방지)
    df.to_csv(args.out, index=False, encoding="utf-8-sig")

    # 처리 완료 메시지 + 일부 데이터 출력
    print(f"완료: {len(df)}개 항목을 '{args.out}'에 저장했습니다.")
    print(df.head(10).to_string(index=False))


# -----------------------------------------------------------
# 프로그램 시작점
# -----------------------------------------------------------
if __name__ == "__main__":
    main()