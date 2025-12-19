import requests
import pandas as pd
import numpy as np # [속도 향상 핵심]
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

# 전국 지역코드
zcodes = [
    '11', '26', '27', '28', '29', '30', '31', '36', 
    '41', '43', '44', '46', '47', '48', '50', '51', '52'
]

# ==========================================
# [매핑 및 함수]
# ==========================================
REGION_MAP = {
    '11': '서울특별시', '26': '부산광역시', '27': '대구광역시', '28': '인천광역시',
    '29': '광주광역시', '30': '대전광역시', '31': '울산광역시', '36': '세종특별자치시',
    '41': '경기도', '43': '충청북도', '44': '충청남도', '46': '전라남도',
    '47': '경상북도', '48': '경상남도', '50': '제주특별자치도', '51': '강원특별자치도',
    '52': '전북특별자치도'
}

BUSI_MAP = {'ME': '환경부', 'LU': 'LG유플러스', 'SG': '시그넷', 'KP': '한국전력'} 

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

def send_slack_alert(message):
    if not slack_webhook_url:
        print("⚠️ 슬랙 웹훅 없음")
        return
    try: requests.post(slack_webhook_url, json={"text": message})
    except: pass

# [핵심] 고속 거리 계산 함수 (NumPy 벡터화)
def calculate_distance_vectorized(lat1, lon1, lat2_series, lon2_series):
    R = 6371
    dlat = np.radians(lat2_series - lat1)
    dlon = np.radians(lon2_series - lon1)
    a = np.sin(dlat/2)**2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2_series)) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c

# ==========================================
# 0. 인증키 확인
# ==========================================
if not service_key:
    print("❌ API 인증키 없음")
    exit()

# ==========================================
# 1. 오늘 데이터 수집 (재시도 로직 강화)
# ==========================================
all_data = []
print("📡 데이터 수집 시작 (안정성 강화 모드)")

for zcode in zcodes:
    page_no = 1
    
    while True:
        # [핵심] 3번까지 재시도 (Retry Logic)
        success = False
        for attempt in range(3):
            try:
                # 타임아웃 30초로 넉넉하게 설정
                res = requests.get(base_url, params={"pageNo": page_no, "numOfRows": "9999", "zcode": zcode, "dataType": "JSON"}, timeout=30)
                
                if res.status_code == 200:
                    try:
                        data = res.json()
                        items = data.get('items', {}).get('item', [])
                        if isinstance(items, dict): items = [items]
                        
                        # 아이템이 없으면 해당 지역 수집 종료 (정상)
                        if not items:
                            success = True
                            break 
                        
                        all_data.extend(items)
                        print(f"지역 {zcode} - {page_no}페이지: {len(items)}건 수집")
                        
                        if len(items) < 9999:
                            # 마지막 페이지 도달
                            page_no = -1 # 루프 종료 신호
                        else:
                            page_no += 1 # 다음 페이지
                        
                        success = True
                        break # 성공했으므로 재시도 루프 탈출
                        
                    except json.JSONDecodeError:
                        print(f"⚠️ JSON 파싱 에러 (지역 {zcode}, 페이지 {page_no}) - 재시도 {attempt+1}/3")
                        time.sleep(2)
                else:
                    print(f"⚠️ 서버 에러 {res.status_code} (지역 {zcode}) - 재시도 {attempt+1}/3")
                    time.sleep(3)
            except Exception as e:
                print(f"⚠️ 연결 에러: {e} (지역 {zcode}) - 재시도 {attempt+1}/3")
                time.sleep(3)
        
        # 3번 다 실패했거나, 마지막 페이지(-1)인 경우 처리
        if not success:
            print(f"❌ 지역 {zcode} {page_no}페이지 수집 실패. 다음 지역으로 이동합니다.")
            break
        
        if page_no == -1:
            break
            
        time.sleep(0.2) # 서버 부하 방지용 짧은 대기

if not all_data:
    print("❌ 수집된 데이터가 하나도 없습니다.")
    exit()

print(f"✅ 총 {len(all_data)}건 수집 완료!")

df = pd.DataFrame(all_data)

# 가공
df['권역'] = df['zcode'].apply(classify_region)
df['지역명'] = df['zcode'].map(REGION_MAP).fillna(df['zcode'])
df['운영기관(가공)'] = df['busiId'].map(BUSI_MAP).fillna(df['busiNm'])
df['newtype'] = df.apply(classify_charger_newtype, axis=1)
df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
df['calc_capacity'] = df.apply(get_capacity_value, axis=1)

# 컬럼 정리
cols = df.columns.tolist()
front = ['권역', '지역명', '운영기관(가공)', 'newtype', 'statNm', 'addr', 'chgerType', 'output']
final = [c for c in front if c in cols] + [c for c in cols if c not in front]
df = df[final]

# 오늘 데이터 저장
today_str = datetime.now().strftime("%Y%m%d")
df.to_excel(f"전기차충전소_{today_str}.xlsx", index=False)

# ==========================================
# 2. 신규 감지 (속도 개선)
# ==========================================
new_chargers_df = pd.DataFrame()
prev_df = pd.DataFrame()

# [속도 개선] low_memory=False로 로딩
if os.path.exists(prev_data_path_gz):
    print("📂 (압축) 어제 데이터 로드")
    prev_df = pd.read_csv(prev_data_path_gz, compression='gzip', low_memory=False)
elif os.path.exists(prev_data_path_csv):
    print("📂 (일반) 어제 데이터 로드")
    prev_df = pd.read_csv(prev_data_path_csv, low_memory=False)

if not prev_df.empty:
    prev_ids = set(prev_df['statId'].astype(str))
    curr_ids = set(df['statId'].astype(str))
    new_ids = curr_ids - prev_ids
    
    if new_ids:
        print(f"✨ 신규 {len(new_ids)}개소 발견 (전체 스캔 시작)")
        new_chargers_df = df[df['statId'].astype(str).isin(new_ids)].copy()
    else:
        print("✅ 신규 없음")
else:
    print("⚠️ 비교 파일 없음 -> 전체 데이터를 대상으로 분석 (최초 실행)")
    new_chargers_df = df.copy()

# ==========================================
# 3. 거리 계산 (벡터화 + 그룹핑)
# ==========================================
alert_list = []
history_records = []
today_dash = datetime.now().strftime("%Y-%m-%d")

if not new_chargers_df.empty and os.path.exists(skel_file_path):
    skel_df = pd.read_csv(skel_file_path)
    
    # 1. 신규 중 '급속'만 필터링
    targets = new_chargers_df[new_chargers_df['newtype'] == '급속'].copy()
    
    if not targets.empty:
        # 2. [로직 개선] statId 기준 그룹핑 (용량 합산, 중복 제거)
        agg_rules = {
            'calc_capacity': 'sum',
            'statNm': 'first', '운영기관(가공)': 'first',
            'addr': 'first', 'lat': 'first', 'lng': 'first'
        }
        grouped_targets = targets.groupby('statId', as_index=False).agg(agg_rules)
        
        print(f"🚀 분석 대상: {len(grouped_targets)}개 충전소 (고속 계산 중...)")
        
        # 3. [속도 개선] SKEL 지점 루프 + 벡터화 거리 계산
        for _, skel in skel_df.iterrows():
            s_lat, s_lng = skel.get('lat'), skel.get('lng')
            if pd.isna(s_lat) or pd.isna(s_lng): continue

            # NumPy를 이용한 고속 거리 계산
            distances = calculate_distance_vectorized(s_lat, s_lng, grouped_targets['lat'], grouped_targets['lng'])
            
            # 1km 이내 인덱스 추출
            nearby_indices = np.where(distances <= 1.0)[0]
            
            for idx in nearby_indices:
                dist = distances[idx]
                comp = grouped_targets.iloc[idx]
                
                alert_info = {
                    "skel_name": skel['statNm'], "dist": f"{dist:.3f}km",
                    "comp_name": comp['statNm'], "comp_busi": comp['운영기관(가공)'],
                    "output": comp['calc_capacity'], "addr": comp['addr']
                }
                alert_list.append(alert_info)
                
                history_records.append({
                    "감지일자": today_dash,
                    "SKEL_ID": skel.get('statId', 'Unknown'), "SKEL_지점명": skel.get('statNm', 'Unknown'),
                    "거리(km)": round(dist, 3), 
                    "경쟁사_ID": comp['statId'], "경쟁사_지점명": comp['statNm'],
                    "운영사": comp['운영기관(가공)'], "총용량": comp['calc_capacity'],
                    "경쟁사_주소": comp['addr']
                })
        print("✅ 거리 계산 완료")

# 결과 저장
if history_records:
    new_h = pd.DataFrame(history_records)
    if os.path.exists(history_file_path):
        old_h = pd.read_csv(history_file_path)
        final_h = pd.concat([old_h, new_h], ignore_index=True)
    else: final_h = new_h
    final_h.to_csv(history_file_path, index=False, encoding='utf-8-sig')

# 슬랙 전송
if alert_list:
    msg = f"🚨 *[경쟁사 진입] SKEL 반경 1km 내 ({today_dash})*\n총 {len(alert_list)}건 감지\n\n"
    for item in alert_list[:15]:
        msg += f"📍 *{item['skel_name']}* 인근 ({item['dist']})\n • {item['comp_name']} ({item['comp_busi']}) / 총 {item['output']}kW\n"
    if len(alert_list) > 15:
        msg += f"\n...외 {len(alert_list)-15}건 (엑셀 확인)"
    send_slack_alert(msg)

# 데이터 압축 저장
df.to_csv(prev_data_path_gz, index=False, compression='gzip', encoding='utf-8-sig')
print(f"💾 데이터 갱신 완료: {prev_data_path_gz}")

if os.path.exists(prev_data_path_csv):
    try: os.remove(prev_data_path_csv)
    except: pass
