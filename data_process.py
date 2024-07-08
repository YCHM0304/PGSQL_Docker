import pandas as pd
import psycopg2
from io import StringIO
import csv

# 資料庫連接參數
host = "localhost"
dbname = "daily_result_bth"
user = "postgres"
password = "admin"

# CSV 文件路徑
csv_file_path = './daily_result_bth.csv'

# 使用 pandas 讀取 CSV 文件
df = pd.read_csv(csv_file_path, usecols=['user_id', 'report_time', 'update_time', 'kwh', 'appliance_kwh'])

# 修正逗號分隔的數據列
df['appliance_kwh'] = df['appliance_kwh'].apply(lambda x: '"' + x + '"')

# 建立資料庫連接
conn = psycopg2.connect(f"host={host} dbname={dbname} user={user} password={password}")
conn.autocommit = True
cursor = conn.cursor()

# 創建表格（根據需要調整字段名稱和類型）
create_table_query = """
CREATE TABLE IF NOT EXISTS daily_result_bth (
    user_id VARCHAR(50),
    report_time TIMESTAMP,
    update_time TIMESTAMP,
    kwh NUMERIC,
    appliance_kwh TEXT
);
"""
cursor.execute(create_table_query)

# 使用 StringIO 將 pandas DataFrame 轉為 CSV 格式，然後導入 PostgreSQL
output = StringIO()
df.to_csv(output, sep='\t', header=False, index=False, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
output.seek(0)

# cursor.copy_from(output, 'daily_result_bth', sep='\t', null="")  # 確保表格名稱與列匹配
cursor.copy_from(output, 'daily_result_bth', columns=('user_id', 'report_time', 'update_time', 'kwh', 'appliance_kwh'))

# 關閉連接
cursor.close()
conn.close()
