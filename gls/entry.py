import pandas as pd
import csv
from fire import Fire
import psycopg2
import logging
from sqlalchemy import create_engine


def connect_to_postgres():
    conn = create_engine("postgresql://accounting:unicorn@nextcloud-do:5432/accounting")
    return conn


def load_to_df(filename: str):
    with open(filename, encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        df_l = []
        for row in reader:
            if len(row) > 0 and row[0] == 'Bezeichnung Auftragskonto':
                df_l.append(row)
                continue

            if len(df_l) > 0 and len(row) == 0:
                break

            if len(df_l) > 0 and len(row[1]) > 0:
                df_l.append(row)

        return pd.DataFrame(df_l[1:], columns=df_l[0])



def main(filename: str):
    conn = connect_to_postgres()
    df = load_to_df(filename)
    # convert the column names to lower case and replace spaces with underscores
    df.columns = df.columns.map(lambda x: x.lower().replace(' ', '_'))
    # add current datetime to the dataframe
    df['load_time'] = pd.to_datetime('now')
    df.to_sql("gls", conn, index=False, if_exists='append')


if __name__ == "__main__":
    Fire(main)
