import MySQLdb
import pandas as pd

from datetime import datetime

current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print (current_date)

connection = MySQLdb.connect(
    host='localhost',
    user='test',
    passwd='test',
    db='test')
cursor = connection.cursor()

try:
    cursor.execute("SELECT * FROM user")
    for row in cursor:
        print (row)

    df = pd.read_table('sample.tsv',
        names=['flg', 'name', 'pswd', 'fail', 'type', 'valid'])
    print (df)
    
    for row in df.itertuples():
        print (row)
        if row.flg == 1:
            cursor.execute(f"DELETE FROM user WHERE name = '{row.name}'")
        else:
            cursor.execute(f"""
                INSERT INTO user (name, password, fail_count, type, valid, create_date, update_date)
                    VALUES ('{row.name}', '{row.pswd}', '{row.fail}', '{row.type}', '{row.valid}', '{current_date}', '{current_date}')
                ON DUPLICATE KEY UPDATE
                    password = VALUES(password),
                    fail_count = VALUES(fail_count),
                    type = VALUES(type),
                    valid = VALUES(valid),
                    update_date = VALUES(update_date)
            """)
    connection.commit()
except Exception as e:
    connection.rollback()
    raise e
finally:
    cursor.close()
    connection.close()
