import requests
import pandas as pd
import numpy as np
import time
import os
import json
from datetime import datetime

# ==========================================
# [설정] 인증키, 슬랙 URL
# ==========================================
service_key = os.environ.get("DATA_API_KEY")
slack_webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
base_url = f"http://apis.data.go.kr/B552584/EvCharger/getChargerInfo?serviceKey={service_key}"

# 파일 경로
skel_file_path = "skel_chargers.csv"
history_file_path = "competitor_alerts.csv"
prev_data_path_gz = "latest_data.csv.gz"
prev_data_path_csv = "latest_data.csv"

# 전국 지역코드 [cite: 57, 58]
zcodes = [
    '11', '26', '27', '28', '29', '30', '31', '36', 
    '41', '43', '44', '46', '47', '48', '50', '51', '52'
]

# ==========================================
# [매핑 데이터] 가이드 문서 기반 코드 변환 [cite: 52, 58, 62, 64]
# ==========================================
REGION_MAP = {
    '11': '서울특별시', '26': '부산광역시', '27': '대구광역시', '28': '인천광역시',
    '29': '광주광역시', '30': '대전광역시', '31': '울산광역시', '36': '세종특별자치시',
    '41': '경기도', '43': '충청북도', '44': '충청남도', '46': '전라남도',
    '47': '경상북도', '48': '경상남도', '50': '제주특별자치도', '51': '강원특별자치도',
    '52': '전북특별자치도'
}

BUSI_MAP = {'ME': '환경부', 'LU': 'LG유플러스', 'SG': '시그넷', 'KP': '한국전력'} 

# [추가] 3.6. kind (충전소 구분 코드) 매핑 [cite: 61, 62]
KIND_MAP = {
    'A0': '공공시설', 'B0': '주차시설', 'C0': '휴게시설', 'D0': '관광시설', 'E0': '상업시설',
    'F0': '차량정비시설', 'G0': '기타시설', 'H0': '공동주택시설', 'I0': '근린생활시설', 'J0': '교육문화시설'
}

# [추가] 3.7. kindDetail (충전소 구분 상세 코드) 매핑 [cite: 63, 64]
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

def send_slack_alert(message):
    if not slack_webhook_url:
        print("⚠️ 슬랙 웹훅 없음")
        return
    try: requests.post(slack_webhook_url, json={"text": message})
    except: pass

# ==========================================
# 1. 오늘 데이터 수집 (안정성 강화 모드)
# ==========================================
if not service_key:
    print("❌ API 인증키 없음")
    exit()

all_data = []
print("📡 데이터 수집 시작")

for zcode in zcodes:
    page_no = 1
    while True:
        success = False
        for attempt in range(3):
            try:
                res = requests.get(base_url, params={"pageNo": page_no, "numOfRows": "9999", "zcode": zcode, "dataType": "JSON"}, timeout=30)
                if res.status_code == 200:
                    try:
                        data = res.json()
                        items = data.get('items', {}).get('item', [])
                        if isinstance(items, dict): items = [items]
                        if not items:
                            success = True
                            break 
                        all_data.extend(items)
                        print(f"지역 {zcode} - {page_no}페이지 수집 중...")
                        if len(items) < 9999: page_no = -1
                        else: page_no += 1
                        success = True
                        break
                    except json.JSONDecodeError:
                        time.sleep(2)
                else:
                    time.sleep(3)
            except Exception as e:
                time.sleep(3)
        
        if not success or page_no == -1: break
        time.sleep(0.2)

if not all_data:
    print("❌ 수집된 데이터 없음")
    exit()

df = pd.DataFrame(all_data)

# ==========================================
# 2. 데이터 가공 (요청하신 신규 컬럼 반영) 
# ==========================================
df['권역'] = df['zcode'].apply(classify_region)
df['지역명'] = df['zcode'].map(REGION_MAP).fillna(df['zcode'])
df['운영기관(가공)'] = df['busiId'].map(BUSI_MAP).fillna(df['busiNm'])
df['newtype'] = df.apply(classify_charger_newtype, axis=1)

# [요청 사항 반영] Kind 및 KindDetail 설명값 추가 
df['Kind(new)'] = df['kind'].map(KIND_MAP).fillna(df['kind'])
df['KindDetail(new)'] = df['kindDetail'].map(KIND_DETAIL_MAP).fillna(df['kindDetail'])

df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
df['calc_capacity'] = df.apply(get_capacity_value, axis=1)

# 컬럼 순서 재배치 (가공 컬럼을 앞쪽으로)
cols = df.columns.tolist()
front = ['권역', '지역명', '운영기관(가공)', 'newtype', 'Kind(new)', 'KindDetail(new)', 'statNm', 'addr']
final = [c for c in front if c in cols] + [c for c in cols if c not in front]
df = df[final]

# 오늘 데이터 저장
today_str = datetime.now().strftime("%Y%m%d")
df.to_excel(f"전기차충전소_{today_str}.xlsx", index=False)
print(f"✅ 총 {len(all_data)}건 수집 및 가공 완료!")

# ==========================================
# 3. 신규 감지 및 경쟁사 분석
# ==========================================
new_chargers_df = pd.DataFrame()
if os.path.exists(prev_data_path_gz):
    prev_df = pd.read_csv(prev_data_path_gz, compression='gzip', low_memory=False)
    new_ids = set(df['statId'].astype(str)) - set(prev_df['statId'].astype(str))
    if new_ids:
        new_chargers_df = df[df['statId'].astype(str).isin(new_ids)].copy()
else:
    new_chargers_df = df.copy()

alert_list = []
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
                alert_list.append({
                    "skel_name": skel['statNm'], "dist": f"{dist:.3f}km",
                    "comp_name": comp['statNm'], "comp_busi": comp['운영기관(가공)'],
                    "output": comp['calc_capacity'], "addr": comp['addr']
                })
                history_records.append({
                    "감지일자": today_dash, "SKEL_지점명": skel.get('statNm'),
                    "거리(km)": round(dist, 3), "경쟁사_지점명": comp['statNm'],
                    "운영사": comp['운영기관(가공)'], "총용량": comp['calc_capacity']
                })

# 결과 저장 및 알림
if history_records:
    new_h = pd.DataFrame(history_records)
    if os.path.exists(history_file_path):
        final_h = pd.concat([pd.read_csv(history_file_path), new_h], ignore_index=True)
    else: final_h = new_h
    final_h.to_csv(history_file_path, index=False, encoding='utf-8-sig')

if alert_list:
    msg = f"🚨 *[경쟁사 진입] SKEL 반경 1km 내 ({today_dash})*\n"
    for item in alert_list[:15]:
        msg += f"📍 *{item['skel_name']}* 인근 ({item['dist']})\n • {item['comp_name']} ({item['comp_busi']}) / {item['output']}kW\n"
    send_slack_alert(msg)

df.to_csv(prev_data_path_gz, index=False, compression='gzip', encoding='utf-8-sig')
print("💾 분석 및 백업 완료")
