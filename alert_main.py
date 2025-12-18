import requests
import pandas as pd
import time
import os
import json
from datetime import datetime
from math import radians, cos, sin, asin, sqrt

# ==========================================
# [설정] 인증키, 슬랙 URL
# ==========================================
service_key = os.environ.get("DATA_API_KEY")
slack_webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
base_url = f"http://apis.data.go.kr/B552584/EvCharger/getChargerInfo?serviceKey={service_key}"

# 파일 경로 설정 (압축 파일 사용)
skel_file_path = "skel_chargers.csv"
history_file_path = "competitor_alerts.csv"
prev_data_path_gz = "latest_data.csv.gz"
prev_data_path_csv = "latest_data.csv"

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

BUSI_MAP = {
    'AC': '아우토크립트', 'AE': '한국자동차환경협회', 'AH': '아하', 'AL': '아론',
    'AM': '아마노코리아', 'AP': '애플망고', 'BA': '부안군', 'BE': '브라이트에너지파트너스',
    'BG': '비긴스', 'BK': '비케이에너지', 'BN': '블루네트웍스', 'BP': '차밥스',
    'BS': '보스시큐리티', 'BT': '보타리에너지', 'CA': '씨에스테크놀로지', 'CB': '참빛이브이씨',
    'CC': '코콤', 'CG': '서울씨엔지', 'CH': '채움모빌리티', 'CI': '쿨사인',
    'CN': '에바씨엔피', 'CO': '한전케이디엔', 'CP': '캐스트프로', 'CR': '크로커스',
    'CS': '한국EV충전서비스센터', 'CT': '씨티카', 'CU': '씨어스', 'CV': '채비',
    'DC': '대성물류건설', 'DE': '대구공공시설관리공단', 'DG': '대구시', 'DL': '딜라이브',
    'DO': '대한송유관공사', 'DP': '대유플러스', 'DR': '두루스코이브이', 'DS': '대선',
    'DY': '동양이엔피', 'E0': '에너지플러스', 'EA': '에바', 'EB': '일렉트리',
    'EC': '이지차저', 'EE': '이마트', 'EG': '에너지파트너즈', 'EH': '이앤에이치에너지',
    'EK': '이노케이텍', 'EL': '엔라이튼', 'EM': 'evmost', 'EN': '이엔',
    'EO': 'E1', 'EP': '이카플러그', 'ER': '이엘일렉트릭', 'ES': '이테스',
    'ET': '이씨티', 'EV': '에버온', 'EX': '이모션플레이스', 'EZ': '차지인',
    'FE': '에프이씨', 'FT': '포티투닷', 'G1': '광주시', 'G2': '광주시',
    'GD': '그린도트', 'GE': '그린전력', 'GG': '강진군', 'GN': '지에스커넥트',
    'GO': '유한회사 골드에너지', 'GP': '군포시', 'GR': '그리드위즈', 'GS': 'GS칼텍스',
    'HB': '에이치엘비생명과학', 'HD': '현대자동차', 'HE': '한국전기차충전서비스', 'HJ': '한진',
    'HL': '에이치엘비일렉', 'HM': '휴맥스이브이', 'HP': '해피차지', 'HR': '한국홈충전',
    'HS': '홈앤서비스', 'HU': '한솔엠에스', 'HW': '한화솔루션', 'HY': '현대엔지니어링',
    'IC': '인천국제공항공사', 'IK': '익산시', 'IM': '아이마켓코리아', 'IN': '신세계아이앤씨',
    'IO': '아이온커뮤니케이션즈', 'IV': '인큐버스', 'JA': '이브이시스', 'JC': '제주에너지공사',
    'JD': '제주특별자치도', 'JE': '제주전기자동차서비스', 'JH': '종하아이앤씨', 'JJ': '전주시',
    'JN': '제이앤씨플랜', 'JT': '제주테크노파크', 'JU': '정읍시', 'KA': '기아자동차',
    'KC': '한국컴퓨터', 'KE': '한국전기차인프라기술', 'KG': 'KH에너지', 'KH': '김해시',
    'KI': '기아자동차', 'KJ': '순천시', 'KL': '클린일렉스', 'KM': '카카오모빌리티',
    'KN': '한국환경공단', 'KO': '이브이파트너스', 'KP': '한국전력공사', 'KR': '이브이씨코리아',
    'KS': '한국전기차솔루션', 'KT': 'KT', 'KU': '한국충전연합', 'L3': '엘쓰리일렉트릭파워',
    'LA': '에스이랩', 'LC': '롯데건설', 'LD': '롯데이노베이트', 'LH': 'LG유플러스 볼트업(플러그인)',
    'LI': '엘에스이링크', 'LT': '광성계측기', 'LU': 'LG유플러스 볼트업', 'MA': '맥플러스',
    'ME': '기후에너지환경부', 'MI': '모니트', 'MO': '매니지온', 'MR': '미래씨앤엘',
    'MS': '미래에스디', 'MT': '모던텍', 'MV': '메가볼트', 'NB': '엔비플러스',
    'NE': '에너넷', 'NH': '농협경제지주 신재생에너지센터', 'NJ': '나주시', 'NN': '이브이네스트',
    'NS': '뉴텍솔루션', 'NT': 'NICE인프라', 'NX': '넥씽', 'OB': '현대오일뱅크',
    'OS': '온스테이션', 'PA': '이브이페이', 'PC': '아이파킹', 'PE': '피앤이시스템즈',
    'PI': 'GS차지비', 'PK': '펌프킨', 'PL': '플러그링크', 'PM': '피라인모터스',
    'PS': '이브이파킹서비스', 'PW': '파워큐브', 'RE': '레드이엔지', 'RS': '리셀파워',
    'S1': '이브이에스이피', 'SA': '설악에너텍', 'SB': '소프트베리', 'SC': '삼척시',
    'SD': '스칼라데이터', 'SE': '서울시', 'SF': '스타코프', 'SG': 'SK시그넷',
    'SH': '에스에이치에너지', 'SJ': '세종시', 'SK': 'SK에너지', 'SL': '에스에스기전',
    'SM': '성민기업', 'SN': '서울에너지공사', 'SO': '선광시스템', 'SP': '스마트포트테크놀로지',
    'SR': 'SK렌터카', 'SS': '투이스이브이씨', 'ST': 'SK일렉링크', 'SU': '순천시 체육시설관리소',
    'SZ': 'SG생활안전', 'TB': '태백시청', 'TD': '타디스테크놀로지', 'TE': '테슬라',
    'TH': '태현교통', 'TL': '티엘컴퍼니', 'TM': '티맵', 'TR': '한마음장애인복지회',
    'TS': '태성콘텍', 'TU': '티비유', 'TV': '아이토브', 'UN': '유니이브이',
    'UP': '유플러스아이티', 'US': '울산시', 'VT': '볼타', 'WB': '이브이루씨',
    'WJ': '우진산전', 'YC': '노란충전', 'YY': '양양군', 'ZE': '이브이모드코리아',
    'ZP': '자몽파워'
}

BUSI_MAP['LU'] = 'LG유플러스'
BUSI_MAP['ME'] = '환경부'
BUSI_MAP['SG'] = '시그넷'

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

def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371
    try:
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
        c = 2 * asin(sqrt(a))
        return R * c
    except: return 9999

def send_slack_alert(message):
    if not slack_webhook_url:
        print("⚠️ 슬랙 웹훅 URL 없음")
        print(message)
        return
    try: requests.post(slack_webhook_url, json={"text": message})
    except: pass

def get_capacity_value(row):
    """
    [신규 로직] 용량 산출: output * method
    method: '단독' -> 1, '동시' -> 0.5, 그 외(None 등) -> 1
    output: 문자열을 숫자로 변환
    """
    try:
        output_val = float(str(row.get('output', 0)).replace(',', '').strip())
    except:
        output_val = 0.0

    method_str = str(row.get('method', '')).strip()
    factor = 0.5 if '동시' in method_str else 1.0
    
    return output_val * factor

# ==========================================
# 0. 필수 설정 확인
# ==========================================
if not service_key:
    print("❌ API 인증키 없음. 종료.")
    exit()

# ==========================================
# 1. 오늘 데이터 수집 (API)
# ==========================================
all_data = []
print("📡 데이터 수집 시작")

for zcode in zcodes:
    page_no = 1
    while True:
        params = {"pageNo": page_no, "numOfRows": "9999", "zcode": zcode, "dataType": "JSON"}
        try:
            response = requests.get(base_url, params=params, timeout=10)
            if response.status_code == 200:
                try:
                    data = response.json()
                    items = data.get('items', {}).get('item', [])
                    if isinstance(items, dict): items = [items]
                    if not items: break
                    all_data.extend(items)
                    print(f"지역 {zcode} - {page_no}페이지: {len(items)}건")
                    if len(items) < 9999: break
                    page_no += 1
                except: break
            else: break
        except: break
        time.sleep(0.5)

if not all_data:
    print("❌ 수집 실패")
    exit()

df = pd.DataFrame(all_data)
# 가공
df['권역'] = df['zcode'].apply(classify_region)
df['지역명'] = df['zcode'].map(REGION_MAP).fillna(df['zcode'])
df['운영기관(가공)'] = df['busiId'].map(BUSI_MAP).fillna(df['busiNm'])
df['newtype'] = df.apply(classify_charger_newtype, axis=1)
df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
# [신규] 용량 계산을 위한 컬럼 추가
df['calc_capacity'] = df.apply(get_capacity_value, axis=1)

# 컬럼 정리
cols = df.columns.tolist()
front = ['권역', '지역명', '운영기관(가공)', 'newtype', 'statNm', 'addr', 'chgerType', 'output']
final = [c for c in front if c in cols] + [c for c in cols if c not in front]
df = df[final]

# 오늘 데이터 엑셀 저장
today_str = datetime.now().strftime("%Y%m%d")
df.to_excel(f"전기차충전소_{today_str}.xlsx", index=False)

# ==========================================
# 2. 신규 감지 (호환성 로직)
# ==========================================
new_chargers_df = pd.DataFrame()
prev_df = pd.DataFrame()

if os.path.exists(prev_data_path_gz):
    print("📂 (압축) 어제 데이터 로드 중...")
    prev_df = pd.read_csv(prev_data_path_gz, compression='gzip')
elif os.path.exists(prev_data_path_csv):
    print("📂 (일반) 어제 데이터 로드 중...")
    prev_df = pd.read_csv(prev_data_path_csv)

if not prev_df.empty:
    prev_ids = set(prev_df['statId'].astype(str))
    curr_ids = set(df['statId'].astype(str))
    new_ids = curr_ids - prev_ids
    
    if new_ids:
        print(f"✨ 신규 {len(new_ids)}개 발견!")
        new_chargers_df = df[df['statId'].astype(str).isin(new_ids)]
    else:
        print("✅ 신규 없음")
else:
    print("⚠️ 어제 데이터 없음. 비교 건너뜀.")

# ==========================================
# 3. 거리 계산 & 알림 & 이력 저장 (중복제거/용량합산 로직 적용)
# ==========================================
alert_list = []
history_records = []
today_dash = datetime.now().strftime("%Y-%m-%d")

if not new_chargers_df.empty and os.path.exists(skel_file_path):
    skel_df = pd.read_csv(skel_file_path)
    # 1. 신규 중 급속만 필터링
    targets = new_chargers_df[new_chargers_df['newtype'] == '급속'].copy()
    
    if not targets.empty:
        # ------------------------------------------------------------------
        # [신규 로직] 경쟁사 ID(statId) 기준으로 GroupBy하여 용량 합산 (1줄 만들기)
        # ------------------------------------------------------------------
        # 필요한 정보만 남겨서 집계
        # - 용량: 합계(sum)
        # - 나머지 정보: 첫번째 값(first) 사용 (지점명, 주소, 위경도 등은 동일하므로)
        agg_rules = {
            'calc_capacity': 'sum',      # 용량은 합산
            'statNm': 'first',           # 지점명은 첫번째 값
            '운영기관(가공)': 'first',    # 운영사도 첫번째 값
            'addr': 'first',             # 주소도 첫번째 값
            'lat': 'first',
            'lng': 'first'
        }
        
        # statId 기준으로 그룹핑
        grouped_targets = targets.groupby('statId', as_index=False).agg(agg_rules)
        
        # 그룹핑된 데이터(충전소 단위)로 거리 계산 반복
        for _, new_stn in grouped_targets.iterrows():
            n_lat, n_lng = new_stn['lat'], new_stn['lng']
            if pd.isna(n_lat) or pd.isna(n_lng): continue
            
            for _, skel in skel_df.iterrows():
                s_lat, s_lng = skel.get('lat'), skel.get('lng')
                dist = calculate_distance(s_lat, s_lng, n_lat, n_lng)
                
                if dist <= 1.0:
                    # 알림 및 이력 저장용 데이터 생성
                    alert_info = {
                        "skel_name": skel['statNm'], "dist": f"{dist:.3f}km",
                        "comp_name": new_stn['statNm'], "comp_busi": new_stn['운영기관(가공)'],
                        "output": new_stn['calc_capacity'], "addr": new_stn['addr']
                    }
                    alert_list.append(alert_info)
                    
                    history_records.append({
                        "감지일자": today_dash,
                        "SKEL_ID": skel.get('statId', 'Unknown'), "SKEL_지점명": skel.get('statNm', 'Unknown'),
                        "거리(km)": round(dist, 3), 
                        "경쟁사_ID": new_stn['statId'], # 그룹핑 기준이었던 statId
                        "경쟁사_지점명": new_stn['statNm'], "운영사": new_stn['운영기관(가공)'],
                        "총용량": new_stn['calc_capacity'], # 합산된 용량
                        "경쟁사_주소": new_stn['addr']
                    })

# 이력 저장
if history_records:
    new_h = pd.DataFrame(history_records)
    if os.path.exists(history_file_path):
        old_h = pd.read_csv(history_file_path)
        final_h = pd.concat([old_h, new_h], ignore_index=True)
    else: final_h = new_h
    final_h.to_csv(history_file_path, index=False, encoding='utf-8-sig')

# 슬랙 전송
if alert_list:
    msg = f"🚨 *[경쟁사 진입] SKEL 반경 1km 내 ({today_dash})*\n\n"
    for item in alert_list:
        msg += f"📍 *{item['skel_name']}* 인근 ({item['dist']})\n • {item['comp_name']} ({item['comp_busi']}) / 총 {item['output']}kW\n"
    send_slack_alert(msg)

# ==========================================
# [중요] 내일 비교용 데이터 압축 저장
# ==========================================
df.to_csv(prev_data_path_gz, index=False, compression='gzip', encoding='utf-8-sig')
print(f"💾 비교용 데이터 압축 저장 완료: {prev_data_path_gz}")

if os.path.exists(prev_data_path_csv):
    try: os.remove(prev_data_path_csv)
    except: pass
