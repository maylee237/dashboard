import os
import pandas as pd
import pymysql
import gspread
import warnings
from tqdm.auto import tqdm
import open_api2
import requests
import xml.etree.ElementTree as ET
warnings.filterwarnings('ignore')

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


try:
    with live_db_conn() as conn:
        sql = """
        SELECT T1.id, T1.business_1, T1.name, T1.naver_id, CONCAT('naver_', T1.naver_id) AS 'business7'
        FROM redtable2021.dic_naver_category T1
        WHERE (T1.name LIKE '%점' OR T1.name LIKE '%점)') AND T1.store_id IS NULL AND T1.naver_id IS NOT NULL
        """
        dic_naver_category = pd.read_sql(sql, conn)

        sql = f"""
        SELECT T1.store_id, T1.business7
        FROM redtable2021.store_contract T1
        WHERE T1.business7 IN {tuple(dic_naver_category["business7"].tolist())}
        """
        store_contract = pd.read_sql(sql, conn)

    df = pd.merge(dic_naver_category, store_contract, on="business7", how="inner")

    with live_db_conn() as conn:
        with conn.cursor() as curs:
            sql = """
            UPDATE redtable2021.dic_naver_category 
            SET store_id = %s
            WHERE id = %s AND business_1 = %s AND name = %s
            """
            val = df.apply(lambda row:(row["store_id"], row["id"], row["business_1"], row["name"]), axis=1).tolist()
            curs.executemany(sql, val)
except:
    print("작업발생X")



with live_db_conn() as conn:
    sql = """
    SELECT T1.id, T1.business_1, T1.name, T1.naver_id, CONCAT('naver_', T1.naver_id) AS 'business7'
    FROM redtable2021.dic_naver_category T1
    WHERE (T1.name LIKE '%점' OR T1.name LIKE '%점)') AND T1.store_id IS NULL AND T1.naver_id IS NOT NULL
    """
    dic_naver_category = pd.read_sql(sql, conn)

try:
    with dev_db_conn() as conn:
        sql = f"""
        SELECT T1.SRC_ID AS 'naver_id', T2.SRC_ID AS 'store_id'
        FROM redtable01.RSTR_CONN T1
        INNER JOIN redtable01.RSTR_CONN T2 ON T1.RSTR_ID = T2.RSTR_ID
        WHERE T1.SRC = 'naver' AND T2.SRC = 'live_db' AND T1.SRC_ID IN {tuple(dic_naver_category["naver_id"].tolist())}
        GROUP BY naver_id
        ORDER BY store_id ASC
        """
        RSTR_CONN = pd.read_sql(sql, conn)

    RSTR_CONN = RSTR_CONN.astype({"naver_id":"str"})

    df1 = pd.merge(dic_naver_category, RSTR_CONN, on="naver_id", how="inner")

    with live_db_conn() as conn:
        with conn.cursor() as curs:
            sql = """
            UPDATE redtable2021.dic_naver_category 
            SET store_id = %s
            WHERE id = %s AND business_1 = %s AND name = %s
            """
            val = df1.apply(lambda row:(row["store_id"], row["id"], row["business_1"], row["name"]), axis=1).tolist()
            curs.executemany(sql, val)
except:
    print("작업발생X")   




file_path = os.path.join(os.environ["HOMEPATH"], "Downloads/")

sa = gspread.service_account(f"{file_path}snappy-cosine-411501-fbfbf5c109c9.json")
sh = sa.open("레드테이블x아이오로라")
wks = sh.worksheet("네이버_제외")
values = wks.get_all_values()
header, rows = values[0], values[1:]
category_need_list = pd.DataFrame(rows, columns=header)

exclude_patterns = ("전문점", "할인점", "가판점", "편의점")
cond1 = category_need_list["상점명"].str.endswith("점")
cond2 = category_need_list["상점명"].str.endswith("점)")
cond3 = ~category_need_list["상점명"].str.endswith(exclude_patterns)
result = category_need_list.loc[(cond1 | cond2) & cond3]




with live_db_conn() as conn:
    """(사업자번호+상점명) 관련 데이터 불러오기"""
    cursor = conn.cursor()
    sql = """
    SELECT category, category_one, category_two
    FROM redtable2021.dic_naver_category
    GROUP BY category_two;
    """
    cursor.execute(sql)
    dic_naver_category = pd.DataFrame(cursor.fetchall())



def fill_two_category(category_one, category_two):
    """카테고리가 없는 것을 채우는 함수"""
    if len(category_two) == 0:
        return category_one
    
    if category_one == category_two and category_one != "":
        return dic_naver_category[dic_naver_category["category_two"] == category_two]["category_one"].reset_index(drop=True)[0]
    
    return category_two

def fill_one_category(category_one, category_two):
    """카테고리가 없는 것을 채우는 함수"""
    if len(category_two) == 0:
        return category_one
    
    if category_one == category_two and category_one != "":
        return dic_naver_category[dic_naver_category["category_two"] == category_two]["category_one"].reset_index(drop=True)[0]
    
    return category_one




final_result = pd.DataFrame() 

if not result.empty:
# 카테고리 채우기
    result["category_two"] = result.apply(lambda row:(fill_two_category(row["category_one"], row["category_two"])),axis=1).tolist()
    result["category_one"] = result.apply(lambda row:(fill_one_category(row["category_one"], row["category_two"])),axis=1).tolist()
    set_idx = dic_naver_category.set_index("category_one")["category"].to_dict()
    result["category"] = result["category_one"].map(set_idx)

    # 빈값 제외하기
    cond1 = result["category_one"] == ""
    cond2 = result["category_two"] == ""
    cond3 = result["address"] == ""
    cond4 = result["category"].isnull()
    final_result = result.loc[~cond1 | ~cond2 | ~cond3 | ~cond4].reset_index(drop=True)
    
    print(final_result)
    # 위경도 채우기
    latitude, longitude = [], []

    for i in tqdm(range(len(final_result))):
        address_text = final_result["address"][i]
        try:
            standard_text = open_api2.preprocess(address_text)

            try:
                latitude.append(standard_text[5])
            except:
                latitude.append("")

            try:
                longitude.append(standard_text[6])
            except:
                longitude.append("")
        except:
            latitude.append("")
            longitude.append("")

    final_result["위도"] = latitude
    final_result["경도"] = longitude

    # 상태값 채우기
    final_result["status"] = 'normal'
    
else :
    final_result = pd.DataFrame()

print(result)
# +
#수정사항

try :
    if not result.empty:
        # dic_naver_category에 넣기
        with live_db_conn() as conn:
            with conn.cursor() as curs:
                sql = """
                INSERT INTO redtable2021.dic_naver_category(business_1, name, category, category_one, category_two, 
                                                    address, address_doro, latitude, longitude, naver_id, `status`)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                val = final_result.apply(lambda row:(row["사업자번호"], row["상점명"], row["category"], row["category_one"], row["category_two"],
                                          row["address"], row["address_doro"], row["위도"], row["경도"], row["naver_id"], row["status"]), axis=1).tolist()
                curs.executemany(sql, val)
except Exception :
    pass


# ++
# 수기작업용
work_one_result = result.loc[cond1 & cond2 & cond3].reset_index(drop=True)

cond1 = category_need_list["상점명"].str.endswith("점")
cond2 = category_need_list["상점명"].str.endswith("점)")
cond3 = ~category_need_list["상점명"].str.endswith(exclude_patterns)

result2 = category_need_list.loc[(~cond1 & ~cond2) | ~cond3]



#공정거래위원회 사업자번호 조회하기

service_key = '1V0uStQwwO8U8iguLFev7OghDFZGC7pJyf6/h6KVXzGckbP70KnlzOw38vH9tgp5u60NPyK48J1EFkyATa/5SA=='
data = []

biznos = result2['사업자번호']
for i in biznos :
    bizno = i
    open_api = f'https://apis.data.go.kr/1130000/MllBs_1Service/getMllBsBiznoInfo_1?serviceKey={service_key}&pageNo=1&numOfRows=10&brno={bizno}'
    res = requests.get(open_api)

    xml_data = res.text
    root = ET.fromstring(xml_data)
    items = root.find('items')


    for item in items.findall('item'):
        opn_sn = item.find('opnSn').text
        bzmnNm = item.find('bzmnNm').text
        brno = item.find('brno').text
        lctnAddr = item.find('lctnAddr').text
        rnAddr = item.find('rnAddr').text
        rprsvNm = item.find('rprsvNm').text
        rprsvEmladr = item.find('rprsvEmladr').text

        data.append({
            'opn_sn': opn_sn,
            'bzmnNm': bzmnNm,
            'brno': brno,
            'lctnAddr': lctnAddr,
            'rnAddr': rnAddr,
            'rprsvNm': rprsvNm,
            'rprsvEmladr': rprsvEmladr
        })

# 데이터프레임으로 변환
df = pd.DataFrame(data)

# 데이터프레임 출력
a = pd.merge(result2,df,left_on='사업자번호',right_on='brno',how='right')

if not a.empty:
# 카테고리 채우기
    a["category_two"] = a.apply(lambda row:(fill_two_category(row["category_one"], row["category_two"])),axis=1).tolist()
    a["category_one"] = a.apply(lambda row:(fill_one_category(row["category_one"], row["category_two"])),axis=1).tolist()
    set_idx = dic_naver_category.set_index("category_one")["category"].to_dict()
    a["category"] = a["category_one"].map(set_idx)

    # 빈값 제외하기
    cond1 = a["category_one"] == ""
    cond2 = a["category_two"] == ""
    cond3 = a["address"] == ""
    cond4 = a["category"].isnull()
    a = a.loc[~cond1 | ~cond2 | ~cond3 | ~cond4].reset_index(drop=True)
    

    # 위경도 채우기
    latitude, longitude = [], []

    for i in tqdm(range(len(a))):
        address_text = a["address"][i]
        try:
            standard_text = open_api2.preprocess(address_text)

            try:
                latitude.append(standard_text[5])
            except:
                latitude.append("")

            try:
                longitude.append(standard_text[6])
            except:
                longitude.append("")
        except:
            latitude.append("")
            longitude.append("")

    a["위도"] = latitude
    a["경도"] = longitude

    # 상태값 채우기
    a["status"] = 'normal'


#a['address'][2].split()[2]
a['address_3'] = a['address_doro'].apply(lambda x : x.split()[2] if len(x) != 0 else None)
a['rnAddr_3'] = a['rnAddr'].apply(lambda x : x.split()[2] if len(x.split()) >= 3 else None)
a['주소일치여부'] = a['rnAddr_3'] == a['address_3']
a.replace('',None,inplace=True)
#mergedf = a[a['address'].isnull()]
set_idx = dic_naver_category.set_index("category_one")["category"].to_dict()
a["category"] = a["category_one"].map(set_idx)


final_result2 = a[a['주소일치여부'] == True]
# dic_naver_category에 넣기

if not final_result2.empty :
    with live_db_conn() as conn:
        with conn.cursor() as curs:
            sql = """
            INSERT INTO redtable2021.dic_naver_category(business_1, name, category, category_one, category_two, 
                                                address, address_doro, latitude, longitude, naver_id, `status`)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            val = final_result2.apply(lambda row:(row["사업자번호"], row["상점명"], row["category"], row["category_one"], row["category_two"],
                                      row["address"], row["address_doro"], row["위도"], row["경도"], row["naver_id"], row["status"]), axis=1).tolist()
            curs.executemany(sql, val)
else :
    pass

b = a[a['주소일치여부'] == False]
b.rename(columns={'lctnAddr':'공정거래위원회_지번주소','rnAddr':'공정거래위원회_도로명주소','bzmnNm':'공정거래위원회_상점명'}, inplace=True)
result3 = pd.merge(result2,b[['사업자번호','공정거래위원회_상점명','공정거래위원회_지번주소','공정거래위원회_도로명주소']],on='사업자번호',how='left')
result3.fillna('',inplace=True)

del work_one_result["category"]
print(work_one_result.columns)

insert_df = pd.concat([work_one_result, result3])
insert_df.replace("NaN" , None, inplace=True)
insert_df.fillna('',inplace=True)

wks.clear()
wks.update("A1",[insert_df.columns.tolist()])
wks.update("A2",insert_df.values.tolist())