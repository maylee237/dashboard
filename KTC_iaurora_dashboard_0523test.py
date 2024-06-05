import os
import time
import pandas as pd
import pymysql
import numpy as np
import gspread
import requests
import json
import warnings
warnings.filterwarnings('ignore')
from oauth2client.service_account import ServiceAccountCredentials
from tqdm import tqdm
from datetime import datetime

def dev_db_conn(): 
    """개발 DB 접속 함수"""
    conn = pymysql.connect( 
        host = 'db-7ma06.pub-cdb.ntruss.com', 
        user = 'redtable', 
        password = 'fpemxpdlqmf5491!@#', 
        autocommit = True, 
        cursorclass = pymysql.cursors.DictCursor) 
    return conn


def live_db_conn(): 
    """Live DB 접속 함수""" 
    conn = pymysql.connect( 
        host = "db-6j3k3.pub-cdb.ntruss.com", 
        user = "redtable", 
        password = "fpemxpdlqmf5491!@#", 
        autocommit = True, 
        cursorclass = pymysql.cursors.DictCursor, 
        db = "redtable2021") 
    return conn



# 이부분 안쓸수있게.
redtable_user_id_list = [123434,124236,69766,124375,135353,
                         27277,139287,139281,125118,139285,
                         139283,139284,59337,103590,70486,
                         143551,143566,143568,143562,128149,
                         56754,139668,146019,27500,135390,
                         27498,141766,143496,108779,125119,
                         64641,153359,112,148912,150208,166796] 


def registerd_card_user_check(user_id, cardSrc):
    """user_id를 통해 내부인원과 씨트립 구별하는 함수"""
    try:
        if isinstance(cardSrc, str):
            return cardSrc

        if isinstance(user_id, int) and user_id in redtable_user_id_list:
            return "내부인원"

      
        return "씨트립"
        
    except:
        return "씨트립"
    
    
######## 채널 대분류 ########
hanyouwang_dict = ["한유망", "한유망 2차", "한유망 3차", "한유망 4차", "한유망 5차",
                 "한유망 6차", "한유망 7차", "한유망 8차"]
foreign_student_dict = ["유학생 프로모션(2만원탑재)_세종대", "유학생 체험단(중국)20만원포함", "유학생", "홍익대 중국인 유학생", "연변대학교",
                       "세종대유학생행사", "세종대", "성신여대", "동국대", "이대", 
                        "외대", "고려대", "안동과학대학교", "건국대", "국민대",
                       "연세대", "한양대", "서울대"]
ctrip_dict = ["씨트립", "ktc_web", "키오스크 직접 발권 인원"]
redtable_dict = ["내부인원", "세일즈마케팅셀 분출"]
jeju_dict = ["제주도"]
sun_international_dict = ["썬인터내셔널"]
marketing_dict = ["오프라인 프로모션", "글로벌마케팅", "글로벌마케팅발대식(50000원충전)", "건대파티배포(10000원충전)입장선착순",
                 "외부홍보용", "문래동 평가단(미스터리쇼퍼)", "영등포관광세일페스타", "한국방문위", "방문위"]
crcc_dict = ["CRCC김예원_홍보", "CRCC_학생1", "CRCC_학생2", "CRCC_학생3", "CRCC김예원_유튜브촬영"]
etc_dict = ["기타", "정보없음", "내부 보관중(세일즈)"]


def channel_main_category(channel_text):
    """채널를 바탕으로 채널 대분류 나누는 함수"""
    channel_dicts = [
        (hanyouwang_dict, "한유망"),
        (foreign_student_dict, "유학생"),
        (ctrip_dict, "씨트립"),
        (redtable_dict, "내부인원"),
        (jeju_dict, "제주도"),
        (sun_international_dict, "썬인터내셔널"),
        (marketing_dict, "마케팅"),
        (crcc_dict, "CRCC"),
        (etc_dict, "기타")
    ]
    
    for channel_dict, category in channel_dicts:
        if channel_text in channel_dict:
            return category
    else:
        return "확인필요"
    
######## 채널 대분류 ########

######## 네이버 카테고리 ########

def request_input_text(input_text):
    """매장명 불필요한 부분 삭제"""
    replacements = {
        "하이디라오코리아(유)":"하이디라오 ",
        "(주)케이에프씨코리아":"KFC ",
        "(주)코리아세븐":"세븐일레븐",
        "씨제이올리브영네트웍스(주)":"올리브영 ",
        "씨제이올리브네트웍스(주)":"올리브영 ",
        "씨제이올리브네트웍스":"올리브영 ",
        "메가엠지씨커피":"메가커피 ",
        "비알코리아(주)":"",
        "씨제이올리브영(주)":"올리브영 ",
        "한국맥도날드(유)":"맥도날드 ",
        "(주)커피빈코리아":"커피빈 ",
        "주식회사 비케이알":"버거킹",
        "(주)파리크라상 ":"",
        "비지에프리테일":"CU",
        "씨유(CU)":"CU",
        "주식회사":"",
        "(주)비케이알":"",
        "(주)":"",
        "(매점)":"",
        "(매장)":""
    }

    for key, value in replacements.items():
        input_text = input_text.replace(key, value)
    return input_text

def get_category_list(df, category_name):
    """각 카테고리별 리스트만드는 함수"""
    return df[df[category_name] != ""][category_name].tolist()

######## 네이버 카테고리 ########

file_path = os.path.join(os.environ["HOMEPATH"], "Downloads/")
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(f"{file_path}snappy-cosine-411501-fbfbf5c109c9.json", scope)
client = gspread.authorize(creds)

####################################
######## 카드번호 가져오가 ########
####################################
IA_KTC_TRANSACTION_EXCEPT_LIST = [
    "계약매장이 아닐때", "- 계약매장이고 0% 페이백으로 되어있는 경우", "페이백X 계약매장이고 5% 페이백으로 되어있는 경우",
    "계약X", "- 계약매장 아닐때 -> 0.2% 페이백", "- 계약매장이고 사업자번호가 2123996827 인 경우",
    "- 계약매장이고 5% 페이백으로 되어있는 경우", "계약매장이고 0% 페이백으로 되어있는 경우", "계약매장이고 5% 페이백으로 되어있는 경우",
    "테스트입니다-ssh-30", "테스트입니다-ssh-0.2", "테스트입니다-일반매장-ssh-0.2",
    "22테스트입니다-일반매장-ssh-0.2", "33테스트입니다-일반매장-ssh-0.2", "44테스트입니다-ssh-0.2",
    "55테스트입니다-ssh-30%", "테스트", "테스트 입금", "테스트 신라", "누적 이벤트2 계약상점O"]

with live_db_conn() as conn:
    """페이카드 데이터 + KTC여부 데이터 추출 쿼리"""
    cursor = conn.cursor()
    sql = f"""
    SELECT T1.id, T1.iapIsdAcnoEcyVl AS 'iaurora_id', T1.iapMskCdno AS 'masking_card_no', RIGHT(T1.iapMskCdno, 4) AS 'iapCdno_4', T1.cnctrYn AS '취소여부', 
    T1.trDt AS '결제날짜', T1.trTm AS '결제시간', T1.trIsttJngbrNm AS '상점명', T1.trIsttJngbrBzprNo AS '사업자번호', T1.trBfRmd AS '결제전잔액', 
    T1.trAmt AS '결제금액', T1.trAfRmd AS '결제후잔액', IFNULL(T1.store_id, "") AS 'store_id', IFNULL(T2.is_ktc, "") AS 'ktc_가맹여부'
    FROM redtable2021.IA_KTC_TRANSACTION T1
    LEFT JOIN redtable2021.store_contract T2 ON T1.store_id = T2.store_id
    WHERE T1.iapAcnRomLnkTrTpcd IS NULL AND T1.trIsttJngbrNm NOT IN {tuple(IA_KTC_TRANSACTION_EXCEPT_LIST)}
    """
    cursor.execute(sql)
    IA_KTC_TRANSACTION = pd.DataFrame(cursor.fetchall())
    #IA_KTC_TRANSACTION = pd.read_sql(sql, conn)

# 취소여부가 된 결제건 제외하기
df_use_list_cancel = IA_KTC_TRANSACTION[IA_KTC_TRANSACTION["취소여부"] == "Y"][["iaurora_id", "결제금액", "상점명"]]
df_use_list_not_cancel = IA_KTC_TRANSACTION[IA_KTC_TRANSACTION["취소여부"] == "N"].drop_duplicates(["iaurora_id", "결제금액", "상점명"])
df_delete = pd.merge(df_use_list_cancel, df_use_list_not_cancel, on=["iaurora_id", "결제금액", "상점명"], how="left").reset_index(drop = True)
IA_KTC_TRANSACTION = pd.concat([IA_KTC_TRANSACTION, df_delete]).drop_duplicates(keep = False).reset_index(drop = True)
IA_KTC_TRANSACTION = IA_KTC_TRANSACTION[IA_KTC_TRANSACTION["취소여부"] == "N"].reset_index(drop = True)

with live_db_conn() as conn:
    """수기 페이카드 리스트"""
    cursor = conn.cursor()
    sql = """
    SELECT T1.iapIsdAcnoEcyVl, T1.cardNo, T1.user_id, T1.usrNo, T1.cardChannel, T1.cardSrc, RIGHT(T1.cardNo, 4) AS 'iapCdno_4', T1.cardDt as '출고일자' , T1.cardYn_confirm_at AS '분출일자'
    FROM redtable2021.IA_KTC_SOURCE T1
    WHERE T1.cardYn = 'Y' AND T1.is_use IN (1,3)
    """
    cursor.execute(sql)
    pay_card_list = pd.DataFrame(cursor.fetchall())
    #pay_card_list = pd.read_sql(sql, conn)

# 수기페이카드 리스트와 매핑
df_card_list = pd.merge(IA_KTC_TRANSACTION, pay_card_list, left_on=["iaurora_id", "iapCdno_4"], 
                        right_on=["iapIsdAcnoEcyVl", "iapCdno_4"], how="left")

'''# 풀카드번호 리스트
sa = gspread.service_account(f"{file_path}snappy-cosine-411501-fbfbf5c109c9.json")
sh = sa.open("레드테이블x아이오로라")
wks = sh.worksheet("카드리스트_NEW")
values = wks.get_all_values()
header, rows = values[0], values[1:]
sheet_card_list = pd.DataFrame(rows, columns=header)
sheet_card_list = sheet_card_list[["카드번호", "출고일자", "분출일자", "분출채널"]]

# 카드번호_구버전 리스트
wks = sh.worksheet("페이카드번호리스트_구버전")
values = wks.get_all_values()
header, rows = values[0], values[1:]
before_sheet_card_list = pd.DataFrame(rows, columns=header)
before_sheet_card_list = before_sheet_card_list[["페이 카드번호", "비고_2", "user_id", "iapCdno_4", "분출일자"]]'''

with live_db_conn() as conn:
    """수기페이카드 리스트를 제외한 KTC 등록카드 리스트"""
    cursor = conn.cursor()
    sql = f"""
    SELECT T1.id, IFNULL(T2.id, "") AS 'user_id', T2.name AS 'user_name', RIGHT(T1.iapCdno, 4) AS '카드 뒷자리 4자리', T1.created_at, T3.iapIsdAcnoEcyVl, T1.iapCdno
    FROM redtable2021.IA_KTC_REGISTERED_CARD T1
    LEFT JOIN redtable2021.user T2 ON T1.user_id = T2.id
    LEFT JOIN redtable2021.IA_KTC_TRANSACTION T3 ON T2.channel_user_no = T3.usrNo
    WHERE T3.iapIsdAcnoEcyVl IS NOT NULL AND T3.iapIsdAcnoEcyVl NOT IN {tuple(pay_card_list["iapIsdAcnoEcyVl"].tolist())}
    GROUP BY T1.id
    order BY T1.id ASC;
    """
    cursor.execute(sql)
    IA_KTC_REGISTERED_CARD = pd.DataFrame(cursor.fetchall())
    #IA_KTC_REGISTERED_CARD = pd.read_sql(sql, conn)
    
# 풀카드번호와 수기페이카드 리스트를 제외한 KTC 등록카드 리스트
mapping_df = pd.merge(IA_KTC_REGISTERED_CARD, df_card_list[['cardNo','출고일자','분출일자','cardSrc', 'cardChannel']] , left_on="iapCdno", right_on="cardNo", how="left")
mapping_df["분출채널"] = mapping_df.apply(lambda row:(registerd_card_user_check(row["user_id"], row["cardSrc"])), axis=1).tolist()
mapping_df.drop_duplicates(subset=['iapIsdAcnoEcyVl','iapCdno','user_name'], inplace=True)


df_add = df_card_list[df_card_list["cardNo"].isnull()].reset_index(drop=True)
#df_add = df_add.drop_duplicates(["iaurora_id", "iapCdno_4"]).reset_index(drop=True)

set_idx = mapping_df.set_index("iapIsdAcnoEcyVl")["분출채널"].to_dict()
df_add["cardSrc"] = df_add["iaurora_id"].map(set_idx)

set_idx = mapping_df.set_index("iapIsdAcnoEcyVl")["iapCdno"].to_dict()
df_add["cardNo"] = df_add["iaurora_id"].map(set_idx)

set_idx = mapping_df.set_index("iapIsdAcnoEcyVl")["user_id"].to_dict()
df_add["user_id"] = df_add["iaurora_id"].map(set_idx)


'''######## 카드번호 4자리와 분출날짜를 바탕으로 카드 매핑 ########
sheet_card_list = sheet_card_list[sheet_card_list["분출일자"] != ""]
sheet_card_list["iapCdno_4"] = sheet_card_list["카드번호"].apply(lambda x:x[-4:])
sheet_card_list = sheet_card_list[["카드번호", "분출채널", "iapCdno_4", "분출일자"]]

before_sheet_card_list["페이 카드번호"] = before_sheet_card_list["페이 카드번호"].apply(lambda x:x.replace(" ","").replace("-",""))
before_sheet_card_list = before_sheet_card_list[(before_sheet_card_list["페이 카드번호"] != "") & (before_sheet_card_list["비고_2"] != "키오스크 직접 발권 인원")]
before_sheet_card_list = before_sheet_card_list[["페이 카드번호", "비고_2", "iapCdno_4", "분출일자"]]
before_sheet_card_list = before_sheet_card_list.rename(columns={"페이 카드번호":"카드번호", "비고_2":"분출채널"})

card_backnumber_dic = pd.concat([sheet_card_list, before_sheet_card_list], axis=0).reset_index(drop=True)
df_add["cardSrc"] = df_add["cardSrc"].fillna("")

for i in range(len(df_add["id"])):
    if df_add["cardSrc"][i] == "":
        df_add_matched_card = card_backnumber_dic[(card_backnumber_dic["iapCdno_4"] == df_add["iapCdno_4"][i]) &
                   (card_backnumber_dic["분출일자"] < df_add["결제날짜"][i])].reset_index(drop=True)
        try:
            df_add["cardSrc"][i] = df_add_matched_card["분출채널"][0]
        except:
            df_add["cardSrc"][i] = "확인필요"'''
            
df_final = pd.concat([df_card_list[df_card_list["cardNo"].notnull()], df_add]).sort_values("id", ascending=True)

del df_final["iapIsdAcnoEcyVl"]
del df_final["usrNo"]
del df_final["취소여부"] ## 취소여부 컬럼을 뺀 이유는 앞에서 전처리시 취소여부 케이스를 다 제외했기 때문

df_final = df_final.rename(columns={"cardNo":"16자리 카드번호", "cardSrc":"분출채널"})

####################################
######## 카드번호 가져오가 ########
####################################


####################################
####### 네이버 카테고리 추가 #######
####################################

with live_db_conn() as conn:
    """페이카드 데이터 + KTC여부 데이터 추출 쿼리"""
    cursor = conn.cursor()
    sql = f"""
    SELECT  business_1 AS '사업자번호', name AS '상점명', category_one,	category_two, address,	address_doro,	naver_id
    FROM redtable2021.dic_naver_category
    """
    cursor.execute(sql)
    naver_category_db = pd.DataFrame(cursor.fetchall())
    #IA_KTC_TRANSACTION = pd.read_sql(sql, conn)
naver_category_db


##### naver_category_sheet 도 테이블로 변경 ok
naver_category_sheet = naver_category_db.rename(columns={"대분류":"category_one", "소분류":"category_two", "address":"주소"}) #

#naver_category_sheet = pd.concat([naver_category_sheet, naver_except])
naver_category_sheet.fillna("", inplace=True)


##### 바꿀부분 상점명 + 사업자번호 추가하기
#store_nm_mapping_list = df_final[~df_final["상점명"].isin(naver_category_sheet["상점명"].tolist())].drop_duplicates("상점명")["상점명"].tolist()
#store_nm_mapping_list

merged_df = pd.merge(df_final, naver_category_sheet, on=['사업자번호', '상점명'], how='left', indicator=True)
result = merged_df[merged_df['_merge'] == 'left_only']
store_nm_mapping_list = result[['사업자번호', '상점명']].drop_duplicates().to_dict('records')


#### 코나 부분 추가

with live_db_conn() as conn:
    """수기페이카드 리스트를 제외한 KTC 등록카드 리스트"""
    cursor = conn.cursor()
    sql = f"""
    SELECT T1.merchantName as 상점명, T1.bizLicenseNo as 사업자번호
    FROM redtable2021.KONA_TRANSACTION T1
    WHERE T1.bizLicenseNo IS NOT NULL;
    """
    cursor.execute(sql)
    KONA_TRANSACTION = pd.DataFrame(cursor.fetchall())
    #IA_KTC_REGISTERED_CARD = pd.read_sql(sql, conn)
KONA_TRANSACTION.drop_duplicates(subset=['사업자번호','상점명'],inplace=True)

kona_merged_df = pd.merge(KONA_TRANSACTION, naver_category_sheet, on=['사업자번호', '상점명'], how='left', indicator=True)
kona_result = kona_merged_df[kona_merged_df['_merge'] == 'left_only']
kona_store_nm_mapping_list = kona_result[['사업자번호', '상점명']].drop_duplicates().to_dict('records')


###
combined_list = kona_store_nm_mapping_list + store_nm_mapping_list
combined_list = list({(d['사업자번호'], d['상점명']): d for d in combined_list}.values())


headers = {'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'}
store_nm_result, naver_id, category_one_result, category_two_result, address_result, roadAddress_result, business_number_result = ([] for _ in range(7))
 
##### 상점명 기준으로 가져온 없는데이터 //// 
for item in combined_list:
    business_number, store_name = item['사업자번호'], item['상점명']
    requestData = requests.get(
        f'https://map.naver.com/p/api/search/allSearch?query={request_input_text(store_name)}&type=all&searchCoord=&boundary=',
        headers=headers
    )
    store_nm_result.append(store_name)
    business_number_result.append(business_number)
    try:
        naver_id.append(json.loads(requestData.text)["result"]["place"]["list"][0]["id"])
    except:
        naver_id.append(None)
    try:
        category_one_result.append(json.loads(requestData.text)["result"]["place"]["list"][0]["category"][0])
    except:
        category_one_result.append(None)
    try:
        category_two_result.append(json.loads(requestData.text)["result"]["place"]["list"][0]["category"][1])
    except:
        category_two_result.append(None)
    try:
        address_result.append(json.loads(requestData.text)["result"]["place"]["list"][0]["address"])
    except:
        address_result.append(None)
    try:
        roadAddress_result.append(json.loads(requestData.text)["result"]["place"]["list"][0]["roadAddress"])
    except:
        roadAddress_result.append(None)
    time.sleep(3)
    

    
dict = {'사업자번호': business_number_result,'상점명': store_nm_result, 'category_one': category_one_result, 'category_two': category_two_result,
       'address': address_result, 'address_doro': roadAddress_result, 'naver_id': naver_id} 

df_naver_search = pd.DataFrame(dict)

#네이버 대분류
sa = gspread.service_account(f"{file_path}snappy-cosine-411501-fbfbf5c109c9.json")
sh = sa.open("레드테이블x아이오로라")

wks = sh.worksheet("네이버_대분류")
values = wks.get_all_values()
header, rows = values[0], values[1:]
naver_to_large_category = pd.DataFrame(rows, columns=header)

# 네이버_제외
sheet02 = sh.worksheet("네이버_제외")
sheet02.clear()

sheet02.update("A1",[df_naver_search.columns.tolist()])
sheet02.update("A2",df_naver_search.values.tolist())



naver_dic = pd.concat([naver_category_sheet, df_naver_search])
naver_dic["상점명"] = naver_dic["상점명"].fillna("")
naver_dic.drop_duplicates(subset=["상점명"], inplace=True)
naver_dic.reset_index(drop=True, inplace=True)
naver_dic = naver_dic.fillna("")


categories = ["식음료비", "기타", "문화/오락비", "쇼핑비", "현지교통비", "의료/뷰티비"]
food_list, etc_list, game_list, shopping_list, trans_list, beauty_list = (
    get_category_list(naver_to_large_category, category) for category in categories
)

def naver_main_category(category_text):
    """네이버 카테고리를 바탕으로 대분류 나누기"""
    category_dicts = [
        (food_list, "식음료"),
        (shopping_list, "쇼핑"),
        (game_list, "문화/오락"),
        (beauty_list, "의료/뷰티"),
        (trans_list, "현지교통"),
        (etc_list, "기타")
    ]

    for category_dict, category in category_dicts:
        if category_text in category_dict:
            return category
    else:
        return "카테고리 추가필요"
    
####################################
####### 네이버 카테고리 추가 #######
####################################


####################################
######## 충전내역 탭 채우기 ########
####################################
wks = sh.worksheet("충전내역")
values = wks.get_all_values()
header, rows = values[0], values[1:]
df_before_charge = pd.DataFrame(rows, columns=header)

with live_db_conn() as conn:
    cursor = conn.cursor()
    sql = """
    SELECT DATE(T1.trDt) AS '거래일자', DATE_FORMAT(TIME(T1.trTm), '%H:%i:%s') AS '시각', T1.iapMskCdno AS '카드번호',
        CASE WHEN T1.iapAcnRomLnkTrTpcd = '43' THEN '위챗페이'
            WHEN T1.iapAcnRomLnkTrTpcd = '31' THEN '키오스크'
            WHEN T1.iapAcnRomLnkTrTpcd = '28' THEN 'PG해외카드'
            WHEN T1.iapAcnRomLnkTrTpcd = '42' THEN 'Alipay'
            WHEN T1.iapAcnRomLnkTrTpcd = '02' THEN 'KB국민카드선불출금취소'
            ELSE '알수없음' END AS '입금유형', 
        T1.trBfRmd AS '거래전잔액', T1.trAmt AS '거래금액', T1.trAfRmd AS '거래후잔액'
    FROM redtable2021.IA_KTC_TRANSACTION T1
    WHERE T1.iapAcnRomLnkTrTpcd IS NOT NULL AND T1.trBfRmd != 0
    """
    cursor.execute(sql)
    df_charge = pd.DataFrame(cursor.fetchall())
    #df_charge = pd.read_sql(sql, conn)
    
df_charge = pd.concat([df_before_charge[:399], df_charge]).reset_index(drop = True)

df_charge["거래전잔액"] = df_charge["거래전잔액"].apply(lambda x:x.replace(",","").replace("₩",""))
df_charge["거래금액"] = df_charge["거래금액"].apply(lambda x:x.replace(",","").replace("₩",""))
df_charge["거래후잔액"] = df_charge["거래후잔액"].apply(lambda x:x.replace(",","").replace("₩",""))

df_charge = df_charge.astype({"거래일자": "str", "시각": "str","거래금액": "int"})

wks.clear()
wks.append_row(df_charge.columns.tolist())
wks.update("A2",df_charge.values.tolist())

####################################
######## 충전내역 탭 채우기 ########
####################################

naver_dic = naver_dic.fillna("")
######## 채널_대분류 ########
#df_final["채널_대분류"] = df_final["분출채널"].apply(lambda x:channel_main_category(x))
df_final["채널_대분류"] = df_final["cardChannel"]
df_final["16자리 카드번호"] = df_final["16자리 카드번호"].fillna("")
df_final.loc[df_final["16자리 카드번호"] == "", "채널_대분류"] = "확인필요"


######## 카테고리_대분류 ########
df_final = pd.merge(df_final, naver_dic, on=["상점명","사업자번호"])
df_final["카테고리_대분류"] = df_final["category_one"].apply(lambda x:naver_main_category(x))
df_final["시/도"] = df_final["주소"].apply(lambda x:x.split()[0] if len(x.split()) > 0 else "")
df_final["시/군/구"] = df_final["주소"].apply(lambda x:x.split()[1] if len(x.split()) > 1 else "")

######## 충전내역 ########
#del df_charge["거래일자"]
#del df_charge["시각"]
df_final= pd.merge(df_final, df_charge, left_on=["masking_card_no","결제전잔액"], right_on=["카드번호","거래후잔액"], how="left")

######## 시트에 넣기 ########
df_final = df_final.replace({np.nan:""})
df_final = df_final[['id', 'iaurora_id', 'masking_card_no', 'iapCdno_4', '16자리 카드번호',
                    '사업자번호', '결제날짜', '결제시간', '시/도', '시/군/구', '카테고리_대분류',
                    'category_one', 'category_two', '상점명', '주소', 'ktc_가맹여부',
                    'store_id', '결제전잔액', '결제금액', '결제후잔액', '채널_대분류',
                    '분출채널', 'user_id', '입금유형', '거래전잔액', '거래금액', 
                    '거래후잔액'
                    ]]

df_final = df_final.drop_duplicates(subset=["id"]).sort_values("id", ascending=True).reset_index(drop=True)
df_final['분출채널'].replace('',None,inplace=True)
df_final['채널_대분류'].replace('',None,inplace=True)
df_final.loc[df_final['채널_대분류'].isna() | (df_final['채널_대분류'] == ''), '채널_대분류'] = df_final.loc[df_final['채널_대분류'].isna() | (df_final['채널_대분류'] == ''), '분출채널'].apply(channel_main_category)
df_final['채널_대분류'].fillna('',inplace=True)
df_final['분출채널'].fillna('정보없음',inplace=True)


wks = sh.worksheet("외부인원_NEW_결제내역")
wks.clear()

wks.update("A1",[df_final.columns.tolist()])
wks.update("A2",df_final.astype(str).values.tolist())

####################################
###### 외부인원_통계모니터링 #######
####################################

####################################
######### 카드 실사용자수 ##########
####################################
with live_db_conn() as conn:
    cursor = conn.cursor()
    sql = """
    SELECT COUNT(DISTINCT T1.iapIsdAcnoEcyVl) AS '실사용자수'
    FROM redtable2021.IA_KTC_TRANSACTION T1;
    """
    cursor.execute(sql)
    data01 = pd.DataFrame(cursor.fetchall())
    
    sql = """
    SELECT SUM(T1.trAmt) AS '충전 금액'
    FROM redtable2021.IA_KTC_TRANSACTION T1
    WHERE T1.rndDscd = 1;
    """
    cursor.execute(sql)
    data02 = pd.DataFrame(cursor.fetchall())
    
    sql = """
    SELECT 
        SUM(CASE WHEN T1.cnctrYn = 'N' THEN T1.trAmt ELSE 0 END) - 
        SUM(CASE WHEN T1.cnctrYn = 'Y' THEN T1.trAmt ELSE 0 END) AS '누적 총 사용 금액'
    FROM redtable2021.IA_KTC_TRANSACTION T1
    WHERE T1.rndDscd = 2;
    """
    cursor.execute(sql)
    data03 = pd.DataFrame(cursor.fetchall())

    sql = """
    SELECT LEFT(T1.trTm,2) AS '시간대', 
         SUM(CASE WHEN T1.rndDscd = 2 AND T1.cnctrYn = 'N' THEN T1.trAmt ELSE 0 END) - 
        SUM(CASE WHEN T1.rndDscd = 2 AND T1.cnctrYn = 'Y' THEN T1.trAmt ELSE 0 END) AS '시간대_결제금액',
        SUM(CASE WHEN T1.rndDscd = 1 AND T1.cnctrYn = 'N' THEN T1.trAmt ELSE 0 END) AS '시간대_충전금액'
    FROM redtable2021.IA_KTC_TRANSACTION T1
    GROUP BY 시간대
    ORDER BY 시간대 ASC;
    """
    cursor.execute(sql)
    data04 = pd.DataFrame(cursor.fetchall())
    del data04["시간대"]
    
    sql = """
    SELECT CONCAT(SUBSTRING(T1.trDt, 1, 4), '-', SUBSTRING(T1.trDt, 5, 2), '-', SUBSTRING(T1.trDt, 7, 2)) AS '결제날짜', 
             COUNT(DISTINCT CASE WHEN T1.rndDscd = 2 THEN T1.iapIsdAcnoEcyVl ELSE 0 END) AS '사용자수',
             SUM(CASE WHEN T1.rndDscd = 2 AND T1.cnctrYn = 'N' THEN T1.trAmt ELSE 0 END) - 
        SUM(CASE WHEN T1.rndDscd = 2 AND T1.cnctrYn = 'Y' THEN T1.trAmt ELSE 0 END) AS '결제금액',
        ROUND((SUM(CASE WHEN T1.rndDscd = 2 AND T1.cnctrYn = 'N' THEN T1.trAmt ELSE 0 END) 
                             - SUM(CASE WHEN T1.rndDscd = 2 AND T1.cnctrYn = 'Y' THEN T1.trAmt ELSE 0 END)) 
                             / COUNT(DISTINCT CASE WHEN T1.rndDscd = 2 THEN T1.iapIsdAcnoEcyVl ELSE 0 END), 0) AS '객단가'
    FROM redtable2021.IA_KTC_TRANSACTION T1
    GROUP BY 결제날짜
    ORDER BY 결제날짜 ASC;
    """
    cursor.execute(sql)
    data05 = pd.DataFrame(cursor.fetchall())
    
    sql = """
    SELECT CONCAT(SUBSTRING(T1.trDt, 1, 4), '-', SUBSTRING(T1.trDt, 5, 2)) AS '결제날짜', 
             SUM(CASE WHEN T1.rndDscd = 2 AND T1.cnctrYn = 'N' THEN T1.trAmt ELSE 0 END) - 
        SUM(CASE WHEN T1.rndDscd = 2 AND T1.cnctrYn = 'Y' THEN T1.trAmt ELSE 0 END) AS '결제금액',
        COUNT(DISTINCT CASE WHEN T1.rndDscd = 2 THEN T1.iapIsdAcnoEcyVl ELSE 0 END) AS '사용자수',
        ROUND((SUM(CASE WHEN T1.rndDscd = 2 AND T1.cnctrYn = 'N' THEN T1.trAmt ELSE 0 END) 
                             - SUM(CASE WHEN T1.rndDscd = 2 AND T1.cnctrYn = 'Y' THEN T1.trAmt ELSE 0 END)) 
                             / COUNT(DISTINCT CASE WHEN T1.rndDscd = 2 THEN T1.iapIsdAcnoEcyVl ELSE 0 END), 0) AS '객단가'
    FROM redtable2021.IA_KTC_TRANSACTION T1
    GROUP BY 결제날짜
    ORDER BY 결제날짜 ASC;
    """
    cursor.execute(sql)
    data06 = pd.DataFrame(cursor.fetchall())
    
    sql = """
    SELECT DATE_FORMAT(T1.order_at, '%Y-%m') AS '월별', ROUND(SUM(T1.order_price) / 7000, 0) AS '코리아트래블카드 구매자수'
    FROM redtable2021.`order` T1
    WHERE T1.ota_id IN (8, 16) AND T1.title LIKE '%Korea Travel Card%' AND T1.`status` = 'paid'
    GROUP BY 월별
    ORDER BY 월별 ASC;
    """
    cursor.execute(sql)
    data07 = pd.DataFrame(cursor.fetchall())



wks = sh.worksheet("외부인원_통계모니터링")
wks.update("C6",data01.astype(int).values.tolist())

traffic_card = int(data01["실사용자수"][0] - df_final["iaurora_id"].nunique())

cell_list = wks.range("C7")
for cell in cell_list:
    cell.value = traffic_card

wks.update_cells(cell_list)

# 누적 충전 금액
wks.update("C8",data02.astype(int).values.tolist())

# 누적 총 사용 금액
wks.update("C9",data03.astype(int).values.tolist())

# 시간대별 결제금액 & 충전금액
wks.update("C40",[data04.columns.tolist()])
wks.update("C41",data04.values.tolist())

# 일자별 결제금액 & 사용자수 & 객단가
data05 = data05.astype({"결제금액":"int", "객단가":"int"})
wks.update("E69",[data05.columns.tolist()])
wks.update("E70",data05.values.tolist())

wks.update("B69",[data05[["결제날짜", "결제금액"]].columns.tolist()])
wks.update("B70",data05[["결제날짜", "결제금액"]].values.tolist())

# 월별 결제금액 & 사용자수 & 객단가
data06 = data06.astype({"결제금액":"int", "객단가":"int"})
wks.update("L69",[data06[["결제금액","사용자수","객단가"]].columns.tolist()])
wks.update("L70",data06[["결제금액","사용자수","객단가"]].values.tolist())

# 씨트립, 페이쥬 코리아트래블 구매자수
data07 = data07.astype({"코리아트래블카드 구매자수":"int"})
wks.update("J119",[data07.columns.tolist()])
wks.update("J120",data07.values.tolist())

# 채널 대분류
df_final = df_final.astype({"결제금액":"int"})
grouped = df_final.groupby("채널_대분류")
df_summary = grouped.agg(
    총_결제금액=pd.NamedAgg(column="결제금액", aggfunc="sum"),
    결제수=pd.NamedAgg(column="iaurora_id", aggfunc="count"),
    결제인원=pd.NamedAgg(column="iaurora_id", aggfunc="nunique"),
).reset_index()

df_summary.sort_values("총_결제금액", ascending=False, inplace=True)
df_summary["결제객단가"] = df_summary.apply(lambda row:(int(round(row["총_결제금액"]/row["결제인원"], 1))), axis=1).tolist()
df_summary.reset_index(drop=True, inplace=True)

wks.update("B13",[df_summary.columns.tolist()])
wks.update("B14",df_summary.values.tolist())

# 카테고리_대분류
grouped = df_final.groupby("카테고리_대분류")
df_summary = grouped.agg(
    사용건수=pd.NamedAgg(column="iaurora_id", aggfunc="count"),
    사용인원=pd.NamedAgg(column="iaurora_id", aggfunc="nunique"),
    총_결제금액=pd.NamedAgg(column="결제금액", aggfunc="sum")
).reset_index()

df_summary["건수비율"] = df_summary["사용건수"].apply(lambda x:x/df_summary["사용건수"].sum())
df_summary["금액비율"] = df_summary["총_결제금액"].apply(lambda x:x/df_summary["총_결제금액"].sum())
df_summary["건당 단가"] = df_summary.apply(lambda row:(round(row["총_결제금액"]/row["사용건수"], 0)),axis=1).tolist()

df_summary.sort_values("총_결제금액", ascending=False, inplace=True)
df_summary.reset_index(drop=True, inplace=True)

wks.update("M13",[df_summary.columns.tolist()])
wks.update("M14",df_summary.values.tolist())

# 네이버 소분류 업데이트
grouped = df_final.groupby("category_two")
df_summary = grouped.agg(
    사용건수=pd.NamedAgg(column="iaurora_id", aggfunc="count"),
    사용인원=pd.NamedAgg(column="iaurora_id", aggfunc="nunique"),
    총_결제금액=pd.NamedAgg(column="결제금액", aggfunc="sum")
).reset_index()

df_summary["건수비율"] = df_summary["사용건수"].apply(lambda x:x/df_summary["사용건수"].sum())
df_summary["금액비율"] = df_summary["총_결제금액"].apply(lambda x:x/df_summary["총_결제금액"].sum())
df_summary["건당 단가"] = df_summary.apply(lambda row:(round(row["총_결제금액"]/row["사용건수"], 0)),axis=1).tolist()

df_summary.sort_values("총_결제금액", ascending=False, inplace=True)
df_summary.reset_index(drop=True, inplace=True)

wks.update("U13",[df_summary.columns.tolist()])
wks.update("U14",df_summary.values.tolist())



# 업데이트 시간 표시
current_time = datetime.now()
formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
time = f"*업데이트 최종 시간 : {formatted_time}"

cell_list = wks.range("A1")
for cell in cell_list:
    cell.value = str(time)

wks.update_cells(cell_list)