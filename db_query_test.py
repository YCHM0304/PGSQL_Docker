import akasha
import akasha.summary as summary
import pandas as pd
import json
import psycopg2
import sqlite3
import pymssql
import mysql.connector

from datetime import datetime
from typing import List, Dict, Union

VERBOSE = True
MODEL = 'openai:gpt-4'
ak = akasha.Doc_QA(model=MODEL, verbose=VERBOSE)

def set_connection_config(sql_type:str, database:str, user:str='', password:str='', host:str='', port:str=''):
    connection_config = {}
    connection_config['SQL_TYPE'] = sql_type
    connection_config['DB_NAME'] = database
    if user:
        connection_config['DB_USER'] = user
    if password:
        connection_config['DB_PASSWORD'] = password
    if host:
        connection_config['DB_HOST'] = host
    if port:
        connection_config['DB_PORT'] = port
    return connection_config

def _get_data(sql_cmd:str, connection_config:Dict[str, str]={}) -> pd.DataFrame:
    sql_type = connection_config.get('SQL_TYPE', 'SQLITE').upper()
    database = connection_config.get('DB_NAME', 'database.db')
    user = connection_config.get('DB_USER', '')
    password = connection_config.get('DB_PASSWORD', '')
    host = connection_config.get('DB_HOST', '')
    port = connection_config.get('DB_PORT', '')
    if sql_type == 'POSTGRESQL':
        conn = psycopg2.connect(
            database=database, 
            user=user, 
            password=password, 
            host=host, 
            port=port
        ) 
    elif sql_type == 'MYSQL':
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
    elif sql_type == 'MSSQL':
        conn = pymssql.connect(
            server=f'{host}:{port}', 
            user=user, 
            password=password, 
            database=database
        )
    elif sql_type == 'SQLITE':
        conn = sqlite3.connect(database)
    else:
        raise ValueError(f'Unsupported SQL_TYPE={sql_type}')
    try:
        # Execute the SQL command and fetch the data
        df = pd.read_sql_query(sql_cmd, conn)
    finally:
        # Ensure the connection is closed
        conn.close()
    return df

def _get_table_schema(table_name:str, connection_config:Dict[str, str]={}) -> pd.DataFrame:
    sql_type = connection_config.get('SQL_TYPE', 'SQLITE').upper()
    database = connection_config.get('DB_NAME', 'database.db')
    if sql_type in ('POSTGRESQL', 'MSSQL'):
        sql = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}';"
    elif sql_type == 'MYSQL':
        sql = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' and table_schema = '{database}';"
    elif sql_type == 'SQLITE':
        sql = f"SELECT name AS column_name, type AS data_type FROM pragma_table_info('{table_name}');"
    else:
        raise ValueError(f'Unsupported SQL_TYPE={sql_type}')
    return _get_data(sql, connection_config=connection_config)

#%% Function
def db_query_func(question: str, table_name: str, column_description_json:Union[str, dict]=None, simplified_answer:bool=False, connection_config:Dict[str,str]={}):
    sql_type = connection_config.get('SQL_TYPE', 'SQLITE').upper()
    # table structure
    table_schema_df = _get_table_schema(table_name=table_name, connection_config=connection_config)
    columns = ','.join(table_schema_df['column_name'].tolist())
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # column description
    if column_description_json is not None:
        try:
            if isinstance(column_description_json, dict):
                column_description = column_description_json
            elif isinstance(column_description_json, str): 
                if column_description_json.endswith('.json'):
                    with open(column_description_json, 'r') as f:
                        column_description = json.load(f)
                else:
                    column_description = json.loads(column_description_json)
        except Exception as e:
            print('Error:', e)
            column_description = {}
    else:
        column_description = {}
    # sample data
    ROW_LIMIT = 1
    ## row where fewest columns are null
    sql_fewest_null_records = 'SELECT {} * FROM {} ORDER BY ({}) ASC {};'.format(
        f'TOP {ROW_LIMIT}' if sql_type == 'MSSQL' else '',
        table_name, 
        '+'.join([f'CASE WHEN {col} IS NULL THEN 1 ELSE 0 END' for col in table_schema_df['column_name']]),
        f'LIMIT {ROW_LIMIT}' if sql_type != 'MSSQL' else ''
        )
    fewest_null_records = _get_data(sql_fewest_null_records, connection_config=connection_config).head(ROW_LIMIT)
     ## row where most columns are null
    sql_most_null_records = 'SELECT {} * FROM {} ORDER BY ({}) ASC {};'.format(
        f'TOP {ROW_LIMIT}' if sql_type == 'MSSQL' else '',
        table_name, 
        '+'.join([f'CASE WHEN {col} IS NULL THEN 0 ELSE 1 END' for col in table_schema_df['column_name']]),
        f'LIMIT {ROW_LIMIT}' if sql_type != 'MSSQL' else ''
        )
    most_null_records = _get_data(sql_most_null_records, connection_config=connection_config).head(ROW_LIMIT)
    sample_data = pd.concat([fewest_null_records, most_null_records], axis=1)
    
    info = {'欄位說明': column_description, 
            '表格結構': dict(zip(table_schema_df['column_name'], table_schema_df['data_type'])),
            '範例資料': sample_data.to_dict(orient='list')} # orient='records'
    sql = ak.ask_self(
        prompt=f'''
        有一資料庫表單={table_name}
        請基於當下時間{current_datetime}, 將用戶的問題={question}
        參考下面之 欄位說明&表格結構&範例資料, 轉為包含{columns}之{sql_type}語法並輸出
        ''',
        info=str(info),
        system_prompt=f'''
        只能產生"查詢資料"的sql語法, 禁止回答其他內容
        ---
        產生的語句必須符合下列範本：
            select [distinct] [top <資料筆數>] [count/sum] {columns} from {table_name} 
            [where <條件1> and <條件2> and ... and <條件n>]  
            [order by <排序欄位> <ASC/DESC>] 
            [limit <資料筆數>]
        ---
        {columns} 和 {table_name} 請直接填寫，不得更換
        []內的內容為選填項目, 可根據問題需求決定是否使用
        <條件>為一個或多個, 以"where"起頭, 並以"and"連接, 目的是用來限縮資料範圍
        <排序欄位>為一個或多個, 以"order by"起頭，並以"ASC"或"DESC"結尾
        ---
        語法使用規範：
        1. 禁止使用group by, having, join, union, insert, update, delete, all等語法，否則罰你10000元
        2. sum：可用於資料型態為數值型的欄位, 且用戶問題中有"加總"的需求, 否則禁止使用
        3. count：可用於用戶問題中有包含"計算筆數"的需求, 否則禁止使用
        4. limit/top：可用於用戶問題中有包含"前幾名"的需求, 否則禁止使用
           limit適用於MySQL, PostgreSQL, SQLITE, top僅適用於MSSQL, 兩者限擇一使用
        5. select 語法僅可出現一次
        ''',
        verbose=VERBOSE
    ) 
    # sql = """select id,user_id,report_time,update_time,kwh,appliance_kwh from daily_result_bth where user_id = 'user_1' and report_time >= '2024-05-01 00:00:00' and report_time < '2024-05-02 00:00:00'"""
    data = _get_data(sql, connection_config=connection_config)
    answer = ak.ask_self(
        prompt=f'''
        請根據資料庫查詢的結果，回答使用者的問題
        ---
        查詢表格：{table_name}
        查詢結果：\n{data}\n
        使用者問題：{question}
        ''',
        info=str(info),
        system_prompt='''
        請直接針對用戶問題進行回答，禁止回答其他內容
        若回答之數據有經過計算過程，請詳述其計算過程
        有關數值的大小比較結果之論述，請自我檢查是否正確再輸出
        ''',
        verbose=VERBOSE
    )
    if simplified_answer:
        answer = ak.ask_self(
            prompt='''請根據下面回答結果，摘取結論''',
            info=answer,
            system_prompt=f'''
            請針對使用者問題：{question}
            取出結論，並以string形式回答
            禁止換句話說
            禁止回答其他內容
            禁止更換數值結果
            數據值不得省略或改為文字論述
            ---
            結論摘取要點：
            1. 排除計算過程
            2. 呈現計算結果
            ''',
            verbose=VERBOSE
        )
    return answer

db_query_tool = akasha.create_tool(
    tool_name='db_query_tool',
    tool_description='''
    This is the tool to answer question based on database query, the parameters are: 
    1. question: str, the question asked by the user, required
    2. table_name: str, the table name to query, required
    3. column_description_json: str, the path of json file which contains description of each columns in the table, 
       or the json string of the description for each column, eg. {"column1": "description1", "column2": "description2"}
       optional, default is None
    4. simplified_answer: bool, whether to simplify the answer, optional, default is False
    5. connection_config: Dict[str, str], the connection configuration of the database 
       including keys such as:(sql_type, database, user, password, host and port), 
       optional, default is {}
    ---
    Please try to find the parameters when using this tool, required parameters must be found, optional parameters can be ignored and use default value if not found.
    the "question" MUST BE THE SAME through the whole process.
    ''',
    func=db_query_func)

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv(override=True)
    
    # FUNCTION TEST
    ## DB QUERY
    question = '請問user_1在5/1用電量最多及最少的電器分別是誰?'
    table_name = 'daily_result_bth'
    column_description_json = '''{
        "user_id": "用戶帳號",
        "report_time": "數據統計日期",
        "kwh": "總用電度數，包含其他電器",
        "appliance_kwh": "各電器用電占比，為string，值內以逗號分隔依序為電視, 冰箱, 冷氣, 開飲機, 洗衣機"
    }'''
    connection_config = set_connection_config(sql_type='SQLITE', database='database.db', user='', password='', host='', port='')
    print(db_query_func(question, table_name, column_description_json, simplified_answer=True, connection_config=connection_config))
    