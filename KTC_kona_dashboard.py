import os
import time
import requests
import json
import pandas as pd
import pymysql
import gspread
from tqdm.auto import tqdm
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

def request_input_text(input_text):
    """매장명 불필요한 부분 삭제"""
    replacements = {
        "하이디라오코리아(유)":"하이디라오 ",
        "(주)케이에프씨코리아":"KFC ",
        "(주)코리아세븐":"세븐일레븐",
        "씨제이올리브영네트웍스(주)":"올리브영 ",
        "씨제이올리브네트웍스(주)":"올리브영 ",
        "씨제이올리브네트웍스":"올리브영 ",
        "씨제이올리브영":"올리브영 ",
        "메가엠지씨커피":"메가커피 ",
        "다이소아성산업":"다이소 ",
        "한국맥도날드 (유)":"맥도날드 ",
        "비알코리아(주)":"",
        "씨제이올리브영(주)":"올리브영 ",
        "씨제이올리브영":"올리브영 ",
        "한국맥도날드(유)":"맥도날드 ",
        "(주)커피빈코리아":"커피빈 ",
        "주식회사 비케이알":"버거킹",
        "비케이알버거킹":"버거킹",
        "BKR버거킹":"버거킹 ",
        "메가엠지씨":"메가커피 ",
        "(주)파리크라상 ":"",
        "비지에프리테일":"CU",
        "씨유(CU)":"CU",
        "주식회사":"",
        "(주)비케이알":"",
        "(주)":"",
        "(매점)":"",
        "(매장)":"",
        "K7 ":"세븐일레븐"
    }

    for key, value in replacements.items():
        input_text = input_text.replace(key, value)
    return input_text

def get_category_list(df, category_name):
    """각 카테고리별 리스트만드는 함수"""
    return df[df[category_name] != ""][category_name].tolist()



#유저 자동화
import numpy as np

with live_db_conn() as conn:
    cursor = conn.cursor()
    sql = """
    SELECT DISTINCT T1.cardNo, T1.merchantName, 
        CONCAT(LEFT(T1.approvalDateTime, 4),'-',RIGHT(LEFT(T1.approvalDateTime, 6),2),'-',RIGHT(LEFT(T1.approvalDateTime, 8),2)) AS 'cardTime',
        T3.cardDt, T3.cardChannel, T3.cardSrc, T6.name AS 'user_country', T3.KONA_CARD_ID
    FROM redtable2021.KONA_TRANSACTION T1
    LEFT JOIN redtable2021.KONA_CARD T2 ON T1.cardNo = T2.card_no
    LEFT JOIN redtable2021.KONA_SOURCE T3 ON T2.id = T3.KONA_CARD_ID
    LEFT JOIN redtable2021.KTC_USER T4 ON T1.userId = T4.card_user_id
    LEFT JOIN redtable2021.user T5 ON T4.user_id = T5.id
    LEFT JOIN redtable2021.IA_KTC_COUNTRY_CODE T6 ON T5.country = T6.code AND T6.lang = 'ko'
    WHERE T1.trType = '01' AND T1.mti = '0100' AND T1.responseCode = '00'
        AND T1.merchantName = '주식회사 레드테이블(환전소3)'
    ORDER BY T1.id ASC
    """
    cursor.execute(sql)
    df = pd.DataFrame(cursor.fetchall())

cond1 = df["cardChannel"] != '내부인원'
cond2 = df["cardChannel"] != '인천공항T1 우리은행'

df = df.loc[cond1 & cond2]
df.reset_index(drop=True, inplace=True)
df.replace({np.nan:None}, inplace=True)

with live_db_conn() as conn:
    with conn.cursor() as curs:
        cursor = conn.cursor()
        sql = """
        UPDATE redtable2021.KONA_SOURCE
        SET cardDt = %s, cardChannel = %s, cardSrc = %s
        WHERE KONA_CARD_ID = %s
        """
        val = df.apply(lambda row:(row["cardTime"], "인천공항T1 우리은행", row["user_country"], row["KONA_CARD_ID"]), axis=1).tolist()
        #curs.executemany(sql, val)
        cursor.executemany(sql, val)
    
# 코나카드 유저 자동화 매핑

with live_db_conn() as conn:
    cursor = conn.cursor()
    sql = """
    SELECT DISTINCT T1.cardNo, T1.id, T1.merchantName,
        CONCAT(LEFT(T1.approvalDateTime, 4),'-',RIGHT(LEFT(T1.approvalDateTime, 6),2),'-',RIGHT(LEFT(T1.approvalDateTime, 8),2)) AS 'cardTime',
        T3.cardDt, T3.cardChannel, T3.cardSrc, T6.name AS 'user_country', T3.KONA_CARD_ID
    FROM redtable2021.KONA_TRANSACTION T1
    LEFT JOIN redtable2021.KONA_CARD T2 ON T1.cardNo = T2.card_no
    LEFT JOIN redtable2021.KONA_SOURCE T3 ON T2.id = T3.KONA_CARD_ID
    LEFT JOIN redtable2021.KTC_USER T4 ON T1.userId = T4.card_user_id
    LEFT JOIN redtable2021.user T5 ON T4.user_id = T5.id
    LEFT JOIN redtable2021.IA_KTC_COUNTRY_CODE T6 ON T5.country = T6.code AND T6.lang = 'ko'
    WHERE T1.trType = '01' AND T1.mti = '0100' AND T1.responseCode = '00' AND T1.userId != 0
        AND T1.merchantName = '주식회사 레드테이블'
    GROUP BY T1.cardNo
    ORDER BY T1.id ASC
    """
    cursor.execute(sql)
    df1 = pd.DataFrame(cursor.fetchall())
    
cond1 = df1["cardDt"].isnull()
cond2 = df1["id"] > 600

df1 = df1.loc[cond1 & cond2]
df1.reset_index(drop=True, inplace=True)
df1.replace({np.nan:None}, inplace=True)

with live_db_conn() as conn:
    with conn.cursor() as cursor:
        for index, row in df1.iterrows():
            # Check if cardChannel is NULL in the database
            check_sql = "SELECT cardChannel FROM redtable2021.KONA_SOURCE WHERE KONA_CARD_ID = %s"
            cursor.execute(check_sql, (row["KONA_CARD_ID"],))
            result = cursor.fetchone()
            cardChannel = result['cardChannel'] if result else None 
            
            if cardChannel is None :
                print("none")
                sql = """
                UPDATE redtable2021.KONA_SOURCE
                SET cardDt = %s, cardChannel = %s, cardSrc = %s
                WHERE KONA_CARD_ID = %s and cardChannel is null
                """
                val = (row["cardTime"], "패키지카드 수령고객", row["user_country"], row["KONA_CARD_ID"])
            else:
                print("yes")
                sql = """
                UPDATE redtable2021.KONA_SOURCE
                SET cardDt = %s, cardSrc = %s
                WHERE KONA_CARD_ID = %s and cardChannel is not null
                """
                val = (row["cardTime"], row["user_country"], row["KONA_CARD_ID"])
            
            cursor.execute(sql, val)




# 필요데이터
file_path = os.path.join(os.environ["HOMEPATH"], "Downloads/")
sa = gspread.service_account(f"{file_path}snappy-cosine-411501-fbfbf5c109c9.json")
iaurora = sa.open("레드테이블x아이오로라")
naver_to_large_category = iaurora.worksheet("네이버_대분류")
values = naver_to_large_category.get_all_values()
header, rows = values[0], values[1:]
naver_to_large_category = pd.DataFrame(rows, columns=header)

with live_db_conn() as conn:
    """페이카드 데이터 + KTC여부 데이터 추출 쿼리"""
    cursor = conn.cursor()
    sql = f"""
    SELECT  business_1 AS '사업자번호', name AS '상점명', category AS '카테고리_대분류' , category_one,	category_two, address,	address_doro,	naver_id
    FROM redtable2021.dic_naver_category
    """
    cursor.execute(sql)
    naver_category_db = pd.DataFrame(cursor.fetchall())
    #IA_KTC_TRANSACTION = pd.read_sql(sql, conn)
naver_category_sheet = naver_category_db


with live_db_conn() as conn:
    cursor = conn.cursor()
    sql = """
    SELECT c.card_no AS 카드번호, k.cardDt AS 분출날짜 , k.cardChannel AS 분출채널 , k.cardSrc AS 분출세부내용
    FROM KONA_SOURCE k
    INNER join KONA_CARD c ON k.KONA_CARD_ID = c.id ;
    """

    cursor.execute(sql)
    KONA_CARD = pd.DataFrame(cursor.fetchall())

    sql = """
    SELECT T1.id, T1.cardNo AS '카드번호', T1.bizLicenseNo AS '사업자번호', 
            LEFT(T1.approvalDateTime, 8) AS '결제날짜', RIGHT(T1.approvalDateTime, 6) AS '결제시간',
            T1.merchantName AS '상점명', T1.address AS '가주소', 
            T1.trAmount AS '결제금액', ROUND(T1.balanceAfter, 0) AS '결제후금액', userId
    FROM redtable2021.KONA_TRANSACTION T1
    WHERE T1.trType = "00" AND T1.mti = "0100" AND T1.authCancelType != "CANCEL" AND T1.responseCode = "00"
        AND T1.id NOT IN (37, 39, 49) AND T1.bizTypeName NOT IN ('테스트');
    """
    cursor.execute(sql)
    KONA_TRANSACTION = pd.DataFrame(cursor.fetchall())

#KONA_CARD = pd.merge(KONA_CARD, df_kona_full_card, on="카드번호", how="left")
KONA_TRANSACTION = pd.merge(KONA_TRANSACTION, KONA_CARD, on="카드번호", how="left")
KONA_TRANSACTION['분출날짜'] = pd.to_datetime(KONA_TRANSACTION['분출날짜'],errors='coerce').dt.date
KONA_TRANSACTION.fillna('', inplace=True)   

### 네이버 카테고리 추가 ###
headers = {'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'}
store_nm_result, naver_id, category_one_result, category_two_result, address_result, roadAddress_result , business_number_result = ([] for _ in range(7))
naver_category_sheet = naver_category_sheet.rename(columns={"대분류":"category_one", "소분류":"category_two", "네이버_주소":"주소"})
store_nm_mapping_list = list(set(KONA_TRANSACTION["상점명"].tolist()))

#store_nm_mapping_list = KONA_TRANSACTION[['사업자번호', '상점명']].drop_duplicates().to_dict('records')
complement = list(set(store_nm_mapping_list) - set(naver_category_sheet["상점명"].tolist()))
filtered_df = KONA_TRANSACTION[KONA_TRANSACTION['상점명'].isin(complement)]
filtered_df.drop_duplicates(subset=['사업자번호', '상점명'],inplace=True)
combined_list = filtered_df[['사업자번호', '상점명']]#.to_dict('records')


    
df_naver_search = iaurora.worksheet("네이버_제외")
values = df_naver_search.get_all_values()
header, rows = values[0], values[1:]
df_naver_search = pd.DataFrame(rows, columns=header)
df_naver_search = df_naver_search[df_naver_search["naver_id"].notnull()].reset_index(drop=True)


# 병합하여 일치하는 행 추출
df_naver_search = pd.merge(df_naver_search, combined_list, on=['사업자번호', '상점명'], how='inner')
#### 이부분 컬럼 변경 필요한지 주시하기

naver_dic = pd.concat([naver_category_sheet, df_naver_search])
naver_dic["상점명"] = naver_dic["상점명"].fillna("")
naver_dic.drop_duplicates(subset=["상점명","사업자번호"], inplace=True)
naver_dic.reset_index(drop=True, inplace=True)
naver_dic = naver_dic.fillna("")


### 네이버 카테고리 추가 ###

df_final = pd.merge(KONA_TRANSACTION, naver_dic,  on=["상점명","사업자번호"], how="left")

'''categories = ["식음료비", "기타", "문화/오락비", "쇼핑비", "현지교통비", "의료/뷰티비"]
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
        return "카테고리 추가필요"'''
    


df_final = df_final.fillna("")

######## 채널_대분류 ########
df_final["채널_대분류"] = df_final["분출채널"].apply(lambda x:channel_main_category(x))

######## 카테고리_대분류 ########
df_final = df_final.rename(columns={"address": "주소"})
#df_final["카테고리_대분류"] = df_final["category_one"].apply(lambda x:naver_main_category(x))
df_final["시/도"] = df_final["주소"].apply(lambda x:x.split()[0] if len(x.split()) > 0 else "")
df_final["시/군/구"] = df_final["주소"].apply(lambda x:x.split()[1] if len(x.split()) > 1 else "")

######## 시트에 넣기 ########
df_final = df_final[['id', '카드번호', '사업자번호', '결제날짜', '결제시간',
                     '시/도', '시/군/구', '카테고리_대분류', 'category_one', 'category_two',
                     '상점명', '주소', '결제금액', '결제후금액', '채널_대분류',
                     '분출채널', '분출세부내용', 'userId']]

df_final = df_final.drop_duplicates(subset=["id"]).sort_values("id", ascending=True).reset_index(drop=True)

columns_name_list = ["DB \nid", "DB \ncardNo", "DB \nbizLicenseNo", "DB \napprovalDateTime 정제", "DB \napprovalDateTime 정제",
                    "네이버", "네이버", "내부데이터", "네이버", "네이버",
                    "DB \nmerchantName", "네이버", "DB \ntrAmount", "DB \nbalanceAfter", "내부데이터",
                    "내부데이터", "내부데이터", "DB \nuserId"]

sh = sa.open("코나아이X코리아트래블카드")
wks = sh.worksheet("결제내역(코나)")
wks.clear()

wks.update("A1",[columns_name_list])
wks.update("A2",[df_final.columns.tolist()])


# 업데이트 에러로 인해 신규 코드작성

df_final = df_final.astype(str)
df_final["id"] = df_final["id"].astype(int)  # id 열을 정수로 변환 (이전에는 문자열이었을 것으로 가정)
df_final = df_final.sort_values("id", ascending=True).reset_index(drop=True) 
df_final = df_final.astype(str)

# 데이터프레임의 크기 계산
num_rows, num_cols = df_final.shape

# 데이터프레임의 값을 A3부터 순차적으로 할당
cell_list = wks.range(f"A3:{chr(ord('A')+num_cols-1)}{num_rows+2}") 


for i, cell in enumerate(cell_list):
    cell.value = df_final.iloc[i // num_cols, i % num_cols]

wks.update_cells(cell_list)


####################################
########## 통계모니터링 ############
####################################

with live_db_conn() as conn:
    cursor = conn.cursor()
    sql = """
    SELECT COUNT(DISTINCT T1.card_no) AS '카드발급수'
    FROM redtable2021.KONA_CARD T1;
    """
    cursor.execute(sql)
    data01 = pd.DataFrame(cursor.fetchall())
    
    sql = """
    SELECT COUNT(DISTINCT T1.cardNo) AS '카드사용수'
    FROM redtable2021.KONA_TRANSACTION T1;
    """
    cursor.execute(sql)
    data02 = pd.DataFrame(cursor.fetchall())
    
    
    sql = """
    SELECT SUM(T1.trAmount) AS '총 충전금액'
    FROM redtable2021.KONA_TRANSACTION T1
    WHERE T1.trType = "01";
    """
    cursor.execute(sql)
    data03 = pd.DataFrame(cursor.fetchall())
    
    sql = """
    SELECT SUM(CASE WHEN T1.trType = '00' THEN T1.trAmount ELSE 0 END) - 
            SUM(CASE WHEN T1.trType = '02' THEN T1.trAmount ELSE 0 END) AS '누적 총 사용 금액'
    FROM redtable2021.KONA_TRANSACTION T1
    """
    cursor.execute(sql)
    data04 = pd.DataFrame(cursor.fetchall())
    
    sql = """
    SELECT LEFT(RIGHT(T1.approvalDateTime, 6), 2) AS '시간대', 
            SUM(CASE WHEN T1.trType = '00' THEN T1.trAmount ELSE 0 END) - 
          SUM(CASE WHEN T1.trType = '02' THEN T1.trAmount ELSE 0 END) AS '시간대_결제금액',
          SUM(CASE WHEN T1.trType = '01' THEN T1.trAmount ELSE 0 END) AS '시간대_충전금액'
    FROM redtable2021.KONA_TRANSACTION T1
    GROUP BY 시간대
    ORDER BY 시간대 ASC;
    """
    cursor.execute(sql)
    data05 = pd.DataFrame(cursor.fetchall())
    
    sql = """
        SELECT CONCAT(LEFT(T1.approvalDateTime, 4), '-', RIGHT(LEFT(T1.approvalDateTime, 6), 2), '-', RIGHT(LEFT(T1.approvalDateTime, 8), 2)) AS '결제날짜',
      SUM(CASE WHEN T1.mti = '0100' THEN T1.trAmount ELSE 0 END) - 
     SUM(CASE WHEN T1.mti = '0400' THEN T1.trAmount ELSE 0 END) AS '결제금액',
     COUNT(DISTINCT CASE WHEN T1.mti = '0100' THEN T1.id ELSE 0 END) AS '결제수',
     COUNT(DISTINCT CASE WHEN T1.mti = '0100' THEN T1.cardNo ELSE 0 END) AS '결제인원',
     ROUND( (SUM(CASE WHEN T1.mti = '0100' THEN T1.trAmount ELSE 0 END) - SUM(CASE WHEN T1.mti = '0400' THEN T1.trAmount ELSE 0 END)) / COUNT(DISTINCT CASE WHEN T1.mti = '0100' THEN T1.cardNo ELSE 0 END), 0) AS '결제객단가'
    FROM redtable2021.KONA_TRANSACTION T1
    WHERE T1.trType = '00' AND T1.responseCode = '00' AND T1.id NOT IN (37, 39, 49, 339)
    GROUP BY 결제날짜
    ORDER BY 결제날짜 ASC;
    """
    cursor.execute(sql)
    data06 = pd.DataFrame(cursor.fetchall())
    
    sql = """
    SELECT CONCAT(LEFT(T1.approvalDateTime, 4), '-', RIGHT(LEFT(T1.approvalDateTime, 6), 2)) AS '결제월', 
        SUM(CASE WHEN T1.trType = '00' THEN T1.trAmount ELSE 0 END) - 
           SUM(CASE WHEN T1.trType = '02' THEN T1.trAmount ELSE 0 END) AS '결제금액',
           COUNT(DISTINCT CASE WHEN T1.trType = '00' THEN T1.id ELSE 0 END) AS '결제수',
           COUNT(DISTINCT CASE WHEN T1.trType = '00' THEN T1.cardNo ELSE 0 END) AS '결제인원',
           ROUND( (SUM(CASE WHEN T1.trType = '00' THEN T1.trAmount ELSE 0 END) - SUM(CASE WHEN T1.trType = '02' THEN T1.trAmount ELSE 0 END)) / COUNT(DISTINCT CASE WHEN T1.trType = '00' THEN T1.cardNo ELSE 0 END), 0) AS '결제객단가'
    FROM redtable2021.KONA_TRANSACTION T1
    GROUP BY 결제월
    ORDER BY 결제월 ASC;
    """
    cursor.execute(sql)
    data07 = pd.DataFrame(cursor.fetchall())
    
    sql = """
    SELECT SUM(CASE WHEN T1.trType = '01' THEN T1.trAmount ELSE 0 END) AS '충전금액',
    COUNT(DISTINCT T1.id) AS '충전수', COUNT(DISTINCT T1.cardNo) AS '충전인원',
    ROUND(SUM(CASE WHEN T1.trType = '01' THEN T1.trAmount ELSE 0 END) / COUNT(DISTINCT T1.cardNo), 0) AS '충전객단가'
    FROM redtable2021.KONA_TRANSACTION T1
    WHERE T1.trType = '01' AND T1.authCancelType != "CANCEL"
    GROUP BY T1.trType
    ORDER BY 충전금액 ASC;
    """
    cursor.execute(sql)
    data08 = pd.DataFrame(cursor.fetchall())


wks = sh.worksheet("통계모니터링(코나)")

# 카드 발행자 수
wks.update("C4",data01.astype(int).values.tolist())

# 카드 총 사용 수
wks.update("C5",data02.astype(int).values.tolist())

# 누적 충전 금액
wks.update("C6",data03.astype(int).values.tolist())

# 누적 총 사용 금액
wks.update("C7",data04.astype(int).values.tolist())

# 채널별 대분류
df_final = df_final.astype({"결제금액":"int"})
grouped = df_final.groupby("채널_대분류")
df_summary = grouped.agg(
    총_결제금액=pd.NamedAgg(column="결제금액", aggfunc="sum"),
    결제수=pd.NamedAgg(column="카드번호", aggfunc="count"),
    결제인원=pd.NamedAgg(column="카드번호", aggfunc="nunique"),
).reset_index()

df_summary.sort_values("총_결제금액", ascending=False, inplace=True)
df_summary["결제객단가"] = df_summary.apply(lambda row:(int(round(row["총_결제금액"]/row["결제인원"], 1))), axis=1).tolist()
df_summary.reset_index(drop=True, inplace=True)

wks.update("B12",df_summary.values.tolist())

### 시간대별

# 데이터프레임의 크기 계산
num_rows, num_cols = data05.shape

# 데이터프레임의 값을 순차적으로 할당할 셀 범위 가져오기
cell_range = wks.range(f"B26:{chr(ord('B') + num_cols - 1)}{26 + num_rows - 1}")

# 데이터프레임의 값을 셀 범위에 할당
for i, cell in enumerate(cell_range):
    cell.value = int(data05.iloc[i // num_cols, i % num_cols])

# 변경된 셀 업데이트
wks.update_cells(cell_range)


### 일별

# 데이터프레임의 크기 계산
num_rows, num_cols = data06.shape

# 데이터프레임의 값을 순차적으로 할당할 셀 범위 가져오기
cell_range = wks.range(f"B53:{chr(ord('B') + num_cols - 1)}{53 + num_rows - 1}")

# 데이터프레임의 값을 셀 범위에 할당
for i, cell in enumerate(cell_range):
    if (i + 1) % num_cols == 1:  # B열(C2)에 대해 문자열로 변환하여 할당
        print(cell.value)
        cell.value = str(data06.iloc[i // num_cols, i % num_cols])
    else:  # 나머지 열은 정수로 변환하여 할당
        cell.value = int(data06.iloc[i // num_cols, i % num_cols])

# 변경된 셀 업데이트
wks.update_cells(cell_range)




### 월별
num_rows, num_cols = data07.shape

# 데이터프레임의 크기 계산
cell_range = wks.range(f"I26:{chr(ord('I') + num_cols - 1)}{26 + num_rows - 1}")

# 데이터프레임의 값을 셀 범위에 할당
for i, cell in enumerate(cell_range):
    if (i + 1) % num_cols == 1:  # B열(C2)에 대해 문자열로 변환하여 할당
        print(cell.value)
        cell.value = str(data07.iloc[i // num_cols, i % num_cols])
    else:  # 나머지 열은 정수로 변환하여 할당
        cell.value = int(data07.iloc[i // num_cols, i % num_cols])

# 변경된 셀 업데이트
wks.update_cells(cell_range)



### 충전
num_rows, num_cols = data08.shape

cell_range = wks.range(f"J12:{chr(ord('J') + num_cols - 1)}{12 + num_rows - 1}")

for i, cell in enumerate(cell_range):
    cell.value = int(data08.iloc[i // num_cols, i % num_cols])

wks.update_cells(cell_range)



# 카테고리_대분류
grouped = df_final.groupby("카테고리_대분류")
df_summary = grouped.agg(
    사용건수=pd.NamedAgg(column="카드번호", aggfunc="count"),
    사용인원=pd.NamedAgg(column="카드번호", aggfunc="nunique"),
    총_결제금액=pd.NamedAgg(column="결제금액", aggfunc="sum")
).reset_index()

df_summary["건수비율"] = df_summary["사용건수"].apply(lambda x:x/df_summary["사용건수"].sum())
df_summary["금액비율"] = df_summary["총_결제금액"].apply(lambda x:x/df_summary["총_결제금액"].sum())
df_summary["건당 단가"] = df_summary.apply(lambda row:(round(row["총_결제금액"]/row["사용건수"], 0)),axis=1).tolist()

df_summary.sort_values("총_결제금액", ascending=False, inplace=True)
df_summary.reset_index(drop=True, inplace=True)

wks.update("P12",df_summary.values.tolist())

# 네이버 소분류 업데이트
grouped = df_final.groupby("category_two")
df_summary = grouped.agg(
    사용건수=pd.NamedAgg(column="카드번호", aggfunc="count"),
    사용인원=pd.NamedAgg(column="카드번호", aggfunc="nunique"),
    총_결제금액=pd.NamedAgg(column="결제금액", aggfunc="sum")
).reset_index()

df_summary["건수비율"] = df_summary["사용건수"].apply(lambda x:x/df_summary["사용건수"].sum())
df_summary["금액비율"] = df_summary["총_결제금액"].apply(lambda x:x/df_summary["총_결제금액"].sum())
df_summary["건당 단가"] = df_summary.apply(lambda row:(round(row["총_결제금액"]/row["사용건수"], 0)),axis=1).tolist()

df_summary.sort_values("총_결제금액", ascending=False, inplace=True)
df_summary.reset_index(drop=True, inplace=True)

wks.update("P26",df_summary.values.tolist())


# 업데이트 시간 표시
current_time = datetime.now()
formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
time = f"*업데이트 최종 시간 : {formatted_time}"

cell_list = wks.range("A1")
for cell in cell_list:
    cell.value = str(time)

wks.update_cells(cell_list)
