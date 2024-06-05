import os
import pandas as pd
import pymysql
import gspread
from datetime import datetime

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

def grouped_function(columns):
    """통계모니터링 함수"""
    grouped = df_final.groupby(columns)
    df_summary = grouped.agg(
        총_결제금액=pd.NamedAgg(column="결제금액", aggfunc="sum"),
        결제수=pd.NamedAgg(column="id", aggfunc="count"),
        결제인원=pd.NamedAgg(column="card_id", aggfunc="nunique")
    ).reset_index()
    df_summary["결제객단가"] = df_summary.apply(lambda row:(round(row["총_결제금액"]/row["결제인원"], 0)),axis=1).tolist()
    df_summary.sort_values("총_결제금액", ascending=False, inplace=True)
    return df_summary


file_path = os.path.join(os.environ["HOMEPATH"], "Downloads/")

sa = gspread.service_account(f"{file_path}snappy-cosine-411501-fbfbf5c109c9.json")

iaurora = sa.open("레드테이블x아이오로라")
df_iaurora = iaurora.worksheet("외부인원_NEW_결제내역")
values = df_iaurora.get_all_values()
header, rows = values[0], values[1:]
df_iaurora = pd.DataFrame(rows, columns=header)

kona = sa.open("코나아이X코리아트래블카드")
df_kona = kona.worksheet("결제내역(코나)")
values = df_kona.get_all_values()
header, rows = values[1], values[2:]
df_kona = pd.DataFrame(rows, columns=header)

def filter_and_assign_promotion(df, address_starts, promotion_name):
    condition = df["주소"].str.startswith(tuple(address_starts))
    columns = ["id", "masking_card_no", "16자리 카드번호", "사업자번호", "결제날짜", 
               "결제시간", "시/도", "시/군/구", "카테고리_대분류", "category_one", 
               "category_two", "상점명", "주소", "결제금액", "결제후잔액",
               "채널_대분류", "분출채널", "iaurora_id"]
    filtered_df = df.loc[condition, columns]
    filtered_df["프로모션"] = promotion_name
    filtered_df["카드형태"] = "아이오로라"
    filtered_df = filtered_df[filtered_df["결제날짜"] > "20240421"]
    return filtered_df

def kona_filter_and_assign_promotion(df, address_starts, promotion_name):
    condition = df["주소"].str.startswith(tuple(address_starts))
    df["masking_card_no"] = df["카드번호"]
    columns = ["id", "masking_card_no", "카드번호", "사업자번호", "결제날짜", 
               "결제시간", "시/도", "시/군/구", "카테고리_대분류", "category_one", 
               "category_two", "상점명", "주소", "결제금액", "결제후금액",
               "채널_대분류", "분출채널", "userId"]
    filtered_df = df.loc[condition, columns]
    filtered_df["프로모션"] = promotion_name
    filtered_df["카드형태"] = "코나"
    filtered_df = filtered_df[filtered_df["결제날짜"] > "20240421"]
    return filtered_df

promotions = {
    "롯데백화점 본점": ["서울특별시 중구 소공동 1", "서울특별시 중구 을지로1가 132-2", "서울특별시 중구 남대문로 81"],
    "롯데백화점 영플라자": ["서울특별시 중구 소공동 123", "서울특별시 중구 남대문로 67"],
    "롯데백화점 잠실점": ["서울특별시 송파구 잠실동 40-1", "서울특별시 송파구 올림픽로 240" , "서울특별시 송파구 송파대로 521"],
    "롯데백화점 캐슬플라자": ["서울특별시 송파구 신천동 7-18", "서울특별시 송파구 올림픽로 269"]
}

def df_preprocessing(df, index):
    # 각 프로모션에 대해 데이터 프레임 생성 및 병합
    df_list = []
    if index == "kona":
        for promotion, addresses in promotions.items():
            df_promotion = kona_filter_and_assign_promotion(df, addresses, promotion)
            df_list.append(df_promotion)
    else:
    
        for promotion, addresses in promotions.items():
            df_promotion = filter_and_assign_promotion(df, addresses, promotion)
            df_list.append(df_promotion)

    # 데이터 프레임 병합
    df_merge = pd.concat(df_list)

    # 중복 제거 및 정렬
    df_merge = df_merge.astype({"id":"int"})
    df_merge.drop_duplicates(subset=["id"], inplace=True)
    df_merge.sort_values("id", ascending=True, inplace=True)
    df_merge.reset_index(drop=True, inplace=True)
    return df_merge


df_merge_iaurora = df_preprocessing(df_iaurora, "iaurora")
df_merge_kona = df_preprocessing(df_kona, "kona")

df_merge_iaurora = df_merge_iaurora.rename(columns={"결제후잔액":"결제후금액", "iaurora_id":"card_id"})
df_merge_kona = df_merge_kona.rename(columns={"카드번호":"16자리 카드번호", "userId":"card_id"})

df_final = pd.concat([df_merge_iaurora, df_merge_kona], axis=0)
df_final.sort_values(by=["결제날짜", "결제시간"], ascending=True, inplace=True)

df_final.reset_index(drop=True, inplace=True)

df_final["결제금액"] = df_final["결제금액"].apply(lambda x: int(str(x).replace("₩","").replace(",","")))
df_final["결제후금액"] = df_final["결제후금액"].apply(lambda x: int(str(x).replace("₩","").replace(",","")))

def business_check(input_text):
    """롯데백화점 4개"""
    if str(input_text) in ("1048126067", "2018513497", "2198500066", "2158524595"):
        return "O"
    return "X"

df_final["롯데사업자_유무"] = df_final["사업자번호"].apply(lambda x:business_check(x))

df_final = df_final[['프로모션', '카드형태', 'id', 'masking_card_no', '16자리 카드번호',
                    '사업자번호', '롯데사업자_유무', '결제날짜', '결제시간', '시/도', 
                    '시/군/구', '카테고리_대분류', 'category_one', 'category_two', '상점명', 
                    '주소', '결제금액', '결제후금액', '채널_대분류', '분출채널', 'card_id']]



wks = kona.worksheet("롯데백화점_결제내역(코나+IA)")
wks.clear()

wks.update("A1",[df_final.columns.tolist()])
wks.update("A2",df_final.values.tolist())

###통계모니터



wks = kona.worksheet("롯데백화점_통계모니터링(코나+IA)")

### 구매고객수
c4value = len(df_final["card_id"].unique())

cell_list = wks.range("C4")
for cell in cell_list:
    cell.value = c4value

wks.update_cells(cell_list)



### 구매건수
c5value = len(df_final)

cell_list = wks.range("C5")
for cell in cell_list:
    cell.value = c5value

wks.update_cells(cell_list)



### 구매금액
c6value = int(df_final["결제금액"].sum())

cell_list = wks.range("C6")
for cell in cell_list:
    cell.value = c6value

wks.update_cells(cell_list)


# 프로모션별
df_summary = grouped_function("프로모션")

wks.update("B12",[df_summary.columns.tolist()])
wks.update("B13",df_summary.values.tolist())

# 채널_대분류별
df_summary = grouped_function("채널_대분류")

wks.update("I12",[df_summary.columns.tolist()])
wks.update("I13",df_summary.values.tolist())

# 결제날짜별
df_summary = grouped_function("결제날짜")
df_summary.sort_values("결제날짜", ascending=True, inplace=True)

wks.update("B23",[df_summary.columns.tolist()])
wks.update("B24",df_summary.values.tolist())

# 매출상위매장
df_summary = grouped_function(["상점명", "프로모션"])

wks.update("I23",[df_summary.columns.tolist()])
wks.update("I24",df_summary.values.tolist())


# 업데이트 시간 표시
current_time = datetime.now()
formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
time = f"*업데이트 최종 시간 : {formatted_time}"

cell_list = wks.range("A1")
for cell in cell_list:
    cell.value = str(time)

wks.update_cells(cell_list)
