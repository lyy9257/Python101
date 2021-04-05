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

## 아파트 거래내역 검색
def get_tradelog():
        
    ### 저장할 데이터베이스 호출(pymysql)
    db_conn = pymysql.connect(
        host = db_ip,
        user = db_id,
        password = db_pw,
        db = 'apart',
        charset = 'utf8'
    )

    sql_syntax = '''
        SELECT 년, 월, 일, 전용면적, 층, 평수, 평당가, 거래금액
        FROM apart.tradelog_강원도
        WHERE 법정동읍면동코드 = 11900 and 아파트 = "춘천뉴시티코아루"
        ORDER BY 년 DESC, 월 DESC, 일 DESC;
    ''' 

    result = pd.read_sql(sql_syntax, db_conn)
    
    return result

# 함수 실행
if __name__ == '__main__':
    result = get_tradelog()
    print(result)
