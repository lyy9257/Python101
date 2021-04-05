import configparser
import pandas as pd
import pymysql

### Read config about database
config = configparser.ConfigParser()
config.read('config.cfg', encoding = 'utf-8')

db_ip = config.get('MYSQL', 'DB_IP')
db_port = config.get('MYSQL', 'DB_PORT')
db_id = config.get('MYSQL', 'DB_ID')
db_pw = config.get('MYSQL', 'DB_PW')

# 아파트 거래내역 검색 - 인자 자유입력
def get_tradelog(area_big, area_small, apart_name):

    ## 저장할 데이터베이스 호출(pymysql)
    db_conn = pymysql.connect(
        host = db_ip,
        user = db_id,
        password = db_pw,
        db = 'apart',
        charset = 'utf8'
    )

    ## 구문작성
    ## SQL Injection 방지를 위해 다음과 같이
    ## 딕셔너리 형태로 포매팅하였음.
    sql_syntax = '''
        SELECT 년, 월, 일, 전용면적, 층, 평수, 평당가, 거래금액
        FROM apart.tradelog_%(area_big)s
        WHERE 법정동읍면동코드 = %(area_small)s and 아파트 = "%(apart_name)s"
        ORDER BY 년 DESC, 월 DESC, 일 DESC;
    ''' %{
        'area_big' : area_big,
        'area_small' : area_small,
        'apart_name' : apart_name,
    }

    result = pd.read_sql(sql_syntax, db_conn)
        
    return result

if __name__ == '__main__':
    result = get_tradelog('광주광', 10800, '소촌동대성베르힐')
    
    print(result)