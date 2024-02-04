import pandas as pd
from fire import Fire
from sqlalchemy import create_engine
import csv

def connect_to_postgres():
    conn = create_engine("postgresql://accounting:unicorn@127.0.0.1:5432/accounting")
    return conn

def main(filename: str):
    conn = connect_to_postgres()
    with open(filename, encoding='utf-8') as f:
        data = []
        columns = None
        parse = False
        reader = csv.reader(f, delimiter=';')
        for row in reader:
            if len(row) == 0:
                if parse:
                    parse = False
                continue
                
            if row[0] == 'Name':  # this is our relevant header row
                parse = True
                columns = row
                continue

            if parse and row[0] == '':  # this is the end of the relevant data
                parse = False
                continue
            
            if parse:
                data.append(row)
                continue

        # end parsing
        columns = [x.strip().lower().replace(' ', '_') for x in columns]
        df = pd.DataFrame(data, columns=columns)
        df = df.drop(columns=[''])
        df['load_time'] = pd.to_datetime('now')
        df.to_sql("gls_depot", conn, index=False, if_exists='append')

    
if __name__ == "__main__":
    Fire(main)
    
