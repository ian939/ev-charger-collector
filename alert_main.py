# 환경부 EV 충전기 정보 일일 수집 + 신규/경쟁사 감지 스크립트 (GitHub Actions에서 매일 실행)
import requests
import pandas as pd
import numpy as np
import time
import os
import json
import sys
from datetime import datetime

# ==========================================
# [설정] 인증키
# ==========================================
service_key = os.environ.get("DATA_API_KEY")
base_url = f"http://apis.data.go.kr/B552584/EvCharger/getChargerInfo?serviceKey={service_key}"

# 파일 경로
skel_file_path = "skel_chargers.csv"
history_file_path = "competitor_alerts.csv"
prev_data_path_gz = "latest_data.csv.gz"

# ==========================================
# [수집 파라미터] 2026-06-17경부터 API 불안정 → 페이지 축소 + 백오프 재시도 + 지역 라운드 재시도
# ==========================================
PAGE_SIZE = 5000            # 페이지당 요청 건수 (9999 → 5000: 대용량 요청 타임아웃 완화)
MAX_ATTEMPTS = 4            # 페이지당 요청 시도 횟수
BACKOFF_SECONDS = [5, 10, 20]  # 재시도 전 대기 (지수 백오프)
MAX_ROUNDS = 3              # 실패 지역 전체 재시도 라운드 수
REQUEST_TIMEOUT = 60        # 요청 타임아웃(초)
MIN_RATIO_VS_PREV = 0.95    # 완전성 게이트: 전일 대비 최소 수집 비율

# 전국 지역코드
zcodes = [
    '11', '26', '27', '28', '29', '30', '31', '36',
    '41', '43', '44', '46', '47', '48', '50', '51', '52'
]

# ==========================================
# [매핑 데이터] 가이드 문서 기반 코드 변환
# ==========================================
REGION_MAP = {
    '11': '서울특별시', '26': '부산광역시', '27': '대구광역시', '28': '인천광역시',
    '29': '광주광역시', '30': '대전광역시', '31': '울산광역시', '36': '세종특별자치시',
    '41': '경기도', '43': '충청북도', '44': '충청남도', '46': '전라남도',
    '47': '경상북도', '48': '경상남도', '50': '제주특별자치도', '51': '강원특별자치도',
    '52': '전북특별자치도'
}

BUSI_MAP = {'ME': '환경부', 'LU': 'LG유플러스', 'SG': '시그넷', 'KP': '한국전력'}

# [추가 요청 사항] busiNm 변경 매핑 규칙
BUSI_NAME_MAP = {
    '기후에너지환경부': '환경부',
    '차지비': 'GS차지비',
    'LG유플러스 볼트업': 'LG유플러스',
    'NICE인프라': '한국전자금융',
    '한국전력공사': '한국전력',
    'SK시그넷': '시그넷'
}

# [추가] 3.6. kind (충전소 구분 코드) 매핑
KIND_MAP = {
    'A0': '공공시설', 'B0': '주차시설', 'C0': '휴게시설', 'D0': '관광시설', 'E0': '상업시설',
    'F0': '차량정비시설', 'G0': '기타시설', 'H0': '공동주택시설', 'I0': '근린생활시설', 'J0': '교육문화시설'
}

# [추가] 3.7. kindDetail (충전소 구분 상세 코드) 매핑
KIND_DETAIL_MAP = {
    'A001': '관공서', 'A002': '주민센터', 'A003': '공공기관', 'A004': '지자체시설',
    'B001': '공영주차장', 'B002': '공원주차장', 'B003': '환승주차장', 'B004': '일반주차장',
    'C001': '고속도로 휴게소', 'C002': '지방도로 휴게소', 'C003': '쉼터',
    'D001': '공원', 'D002': '전시관', 'D003': '민속마을', 'D004': '생태공원', 'D005': '홍보관', 'D006': '관광안내소', 'D007': '관광지', 'D008': '박물관', 'D009': '유적지',
    'E001': '마트(쇼핑몰)', 'E002': '백화점', 'E003': '숙박시설', 'E004': '골프장(CC)', 'E005': '카페', 'E006': '음식점', 'E007': '주유소', 'E008': '영화관',
    'F001': '서비스센터', 'F002': '정비소',
    'G001': '군부대', 'G002': '야영장', 'G003': '공중전화부스', 'G004': '기타', 'G005': '오피스텔', 'G006': '단독주택',
    'H001': '아파트', 'H002': '빌라', 'H003': '사업장(사옥)', 'H004': '기숙사', 'H005': '연립주택',
    'I001': '병원', 'I002': '종교시설', 'I003': '보건소', 'I004': '경찰서', 'I005': '도서관', 'I006': '복지관', 'I007': '수련원', 'I008': '금융기관',
    'J001': '학교', 'J002': '교육원', 'J003': '학원', 'J004': '공연장', 'J005': '관람장', 'J006': '동식물원', 'J007': '경기장'
}

# ==========================================
# [함수 정의]
# ==========================================
def classify_region(code):
    code = str(code)
    if code in ['11', '28', '41']: return '수도권'
    elif code in ['26', '27', '29', '30', '31']: return '5대광역시'
    else: return '지방'

def classify_charger_newtype(row):
    c_type = str(row.get('chgerType', '')).strip()
    output = str(row.get('output', '')).strip()
    slow_types = ['02', '07', '08']
    fast_check_types = ['01', '03', '04', '05', '06', '09', '10']

    if c_type in slow_types: return "완속"
    elif (c_type in fast_check_types) and (output == "30"): return "완속"
    else: return "급속"

def get_capacity_value(row):
    try:
        output_val = float(str(row.get('output', 0)).replace(',', '').strip())
    except:
        output_val = 0.0
    method_str = str(row.get('method', '')).strip()
    factor = 0.5 if '동시' in method_str else 1.0
    return output_val * factor

def calculate_distance_vectorized(lat1, lon1, lat2_series, lon2_series):
    R = 6371
    dlat = np.radians(lat2_series - lat1)
    dlon = np.radians(lon2_series - lon1)
    a = np.sin(dlat/2)**2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2_series)) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c

# ==========================================
# 1. 수집 로직 (실패 은폐 금지: 에러 로깅 + totalCount 대조 + 라운드 재시도)
# ==========================================
def fetch_page(zcode, page_no):
    """한 페이지 수집. 성공 시 (items, totalCount) 반환, MAX_ATTEMPTS 소진 시 RuntimeError.
    실패 원인(HTTP 상태/에러 바디/예외)을 반드시 로그로 남긴다."""
    last_err = ''
    for attempt in range(MAX_ATTEMPTS):
        if attempt:
            time.sleep(BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS) - 1)])
        try:
            res = requests.get(base_url, params={
                "pageNo": page_no, "numOfRows": str(PAGE_SIZE),
                "zcode": zcode, "dataType": "JSON"
            }, timeout=REQUEST_TIMEOUT)
        except Exception as e:
            last_err = f"요청 예외: {type(e).__name__}: {e}"
            print(f"⚠️ 지역 {zcode} p{page_no} 시도 {attempt+1}/{MAX_ATTEMPTS} 실패 — {last_err}")
            continue
        if res.status_code != 200:
            last_err = f"HTTP {res.status_code}: {res.text[:200]}"
            print(f"⚠️ 지역 {zcode} p{page_no} 시도 {attempt+1}/{MAX_ATTEMPTS} 실패 — {last_err}")
            continue
        try:
            data = res.json()
        except json.JSONDecodeError:
            last_err = f"JSON 파싱 실패(게이트웨이 에러 응답 추정): {res.text[:200]}"
            print(f"⚠️ 지역 {zcode} p{page_no} 시도 {attempt+1}/{MAX_ATTEMPTS} 실패 — {last_err}")
            continue
        result_code = str(data.get('resultCode', '00'))
        if result_code != '00':
            last_err = f"resultCode {result_code}: {data.get('resultMsg', '')}"
            print(f"⚠️ 지역 {zcode} p{page_no} 시도 {attempt+1}/{MAX_ATTEMPTS} 실패 — {last_err}")
            continue
        container = data.get('items') or {}
        items = container.get('item', []) if isinstance(container, dict) else []
        if isinstance(items, dict):
            items = [items]
        total = data.get('totalCount')
        return items, (int(total) if total is not None else None)
    raise RuntimeError(last_err or '원인 미상')

def collect_region(zcode):
    """지역 전체 페이지 수집. totalCount와 대조해 완전할 때만 rows 반환, 아니면 RuntimeError."""
    rows = []
    page_no = 1
    total_count = None
    while True:
        items, total = fetch_page(zcode, page_no)
        if total is not None:
            total_count = total
        rows.extend(items)
        print(f"지역 {zcode} - {page_no}페이지 ({len(rows)}/{total_count if total_count is not None else '?'}건)")
        if not items or len(items) < PAGE_SIZE:
            break
        page_no += 1
        time.sleep(0.2)
    if total_count is not None and len(rows) != total_count:
        raise RuntimeError(f"건수 불일치: 수집 {len(rows)}건 != totalCount {total_count}건")
    return rows

def collect_all():
    """전 지역 수집. 실패 지역은 라운드 단위로 재시도하고, 최종 실패가 남으면 exit 1 (불완전 파일 저장 금지)."""
    pending = list(zcodes)
    collected = {}
    for round_no in range(1, MAX_ROUNDS + 1):
        if not pending:
            break
        print(f"— 수집 라운드 {round_no}/{MAX_ROUNDS}: 대상 {pending}")
        failed = []
        for zcode in pending:
            try:
                collected[zcode] = collect_region(zcode)
            except RuntimeError as e:
                print(f"❌ 지역 {zcode} 수집 실패: {e}")
                failed.append(zcode)
        pending = failed
    if pending:
        print(f"❌ 최종 실패 지역 {pending} — 불완전 수집으로 판정, 저장/커밋 중단")
        sys.exit(1)
    all_rows = []
    for z in zcodes:
        all_rows.extend(collected.get(z, []))
    return all_rows

def check_completeness(today_count, prev_count):
    """완전성 게이트: 전일 대비 급감 여부. 전일 기준이 없으면 통과."""
    if not prev_count:
        return True
    return today_count >= prev_count * MIN_RATIO_VS_PREV

# ==========================================
# 메인 흐름
# ==========================================
def main():
    if not service_key:
        print("❌ API 인증키 없음")
        sys.exit(1)

    print("📡 데이터 수집 시작")
    all_data = collect_all()
    if not all_data:
        print("❌ 수집된 데이터 없음")
        sys.exit(1)

    df = pd.DataFrame(all_data)

    # ==========================================
    # 2. 데이터 가공 (요청하신 신규 컬럼 반영)
    # ==========================================
    df['권역'] = df['zcode'].apply(classify_region)
    df['지역명'] = df['zcode'].map(REGION_MAP).fillna(df['zcode'])
    df['운영기관(가공)'] = df['busiId'].map(BUSI_MAP).fillna(df['busiNm'])

    # [요청 반영] NewbusiNm 생성 (busiNm 기준 매핑, 없으면 원본 유지)
    df['NewbusiNm'] = df['bnm'].map(BUSI_NAME_MAP).fillna(df['bnm'])

    df['newtype'] = df.apply(classify_charger_newtype, axis=1)

    # [요청 사항 반영] Kind 및 KindDetail 설명값 추가
    df['Kind(new)'] = df['kind'].map(KIND_MAP).fillna(df['kind'])
    df['KindDetail(new)'] = df['kindDetail'].map(KIND_DETAIL_MAP).fillna(df['kindDetail'])

    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
    df['calc_capacity'] = df.apply(get_capacity_value, axis=1)

    # 컬럼 순서 재배치 (가공 컬럼을 앞쪽으로, NewbusiNm 포함)
    cols = df.columns.tolist()
    front = ['권역', '지역명', '운영기관(가공)', 'NewbusiNm', 'newtype', 'Kind(new)', 'KindDetail(new)', 'statNm', 'addr']
    final = [c for c in front if c in cols] + [c for c in cols if c not in front]
    df = df[final]

    # ==========================================
    # 완전성 게이트: 전일 대비 급감 시 저장/커밋 중단 (2차 안전망)
    # ==========================================
    prev_df = None
    if os.path.exists(prev_data_path_gz):
        prev_df = pd.read_csv(prev_data_path_gz, compression='gzip', low_memory=False)
    prev_count = len(prev_df) if prev_df is not None else 0
    if not check_completeness(len(df), prev_count):
        print(f"❌ 수집 건수 급감: 오늘 {len(df)}건 < 전일 {prev_count}건의 {MIN_RATIO_VS_PREV:.0%} — 불완전 수집으로 판정, 저장/커밋 중단")
        sys.exit(1)

    # 오늘 데이터 저장
    today_str = datetime.now().strftime("%Y%m%d")
    df.to_excel(f"전기차충전소_{today_str}.xlsx", index=False)
    print(f"✅ 총 {len(df)}건 수집 및 가공 완료!")

    # ==========================================
    # 3. 신규 감지 및 경쟁사 분석
    # ==========================================
    new_chargers_df = pd.DataFrame()
    if prev_df is None:
        new_chargers_df = df.copy()
    elif prev_count < len(df) * MIN_RATIO_VS_PREV:
        # 전일 기준선 자체가 불완전하면(과거 불완전 수집분) 재등장 충전기가 전부 신규로 오인됨 → 감지 건너뛰고 기준선만 갱신
        print(f"⚠️ 전일 기준선 불완전({prev_count}건) — 신규 감지 건너뜀, 기준선만 갱신")
    else:
        new_ids = set(df['statId'].astype(str)) - set(prev_df['statId'].astype(str))
        if new_ids:
            new_chargers_df = df[df['statId'].astype(str).isin(new_ids)].copy()

    history_records = []
    today_dash = datetime.now().strftime("%Y-%m-%d")

    if not new_chargers_df.empty and os.path.exists(skel_file_path):
        skel_df = pd.read_csv(skel_file_path)
        targets = new_chargers_df[new_chargers_df['newtype'] == '급속'].copy()

        if not targets.empty:
            # 충전소 ID별 그룹화 (중복 제거 및 용량 합산)
            grouped_targets = targets.groupby('statId', as_index=False).agg({
                'calc_capacity': 'sum', 'statNm': 'first', '운영기관(가공)': 'first',
                'addr': 'first', 'lat': 'first', 'lng': 'first'
            })

            for _, skel in skel_df.iterrows():
                s_lat, s_lng = skel.get('lat'), skel.get('lng')
                if pd.isna(s_lat) or pd.isna(s_lng): continue

                distances = calculate_distance_vectorized(s_lat, s_lng, grouped_targets['lat'], grouped_targets['lng'])
                nearby_indices = np.where(distances <= 1.0)[0]

                for idx in nearby_indices:
                    dist = distances[idx]
                    comp = grouped_targets.iloc[idx]
                    history_records.append({
                        "감지일자": today_dash, "SKEL_지점명": skel.get('statNm'),
                        "거리(km)": round(dist, 3), "경쟁사_지점명": comp['statNm'],
                        "운영사": comp['운영기관(가공)'], "총용량": comp['calc_capacity']
                    })

    # 결과 저장
    if history_records:
        new_h = pd.DataFrame(history_records)
        if os.path.exists(history_file_path):
            final_h = pd.concat([pd.read_csv(history_file_path), new_h], ignore_index=True)
        else: final_h = new_h
        final_h.to_csv(history_file_path, index=False, encoding='utf-8-sig')

    # 기준선 갱신 — 완전성 게이트를 통과한 스냅샷만 여기 도달하므로, 불완전 데이터로 덮어쓰지 않음
    df.to_csv(prev_data_path_gz, index=False, compression='gzip', encoding='utf-8-sig')
    print("💾 분석 및 백업 완료")

if __name__ == "__main__":
    main()
