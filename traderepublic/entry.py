import pandas as pd
import csv
from fire import Fire
import psycopg2
import logging
from sqlalchemy import create_engine


def connect_to_postgres():
    conn = create_engine("postgresql://postgres:unicorn@nixie:5432/accounting")
    return conn


def load_to_df(filename: str):
    # read filename as csv via pandas
    return pd.read_csv(filename, delimiter=';')

def main(filename: str):
    conn = connect_to_postgres()
    df = load_to_df(filename)

    # convert the column names to lower case and replace spaces with underscores
    df.columns = df.columns.map(lambda x: x.lower().replace(' ', '_'))
    # add current datetime to the dataframe
    df['load_time'] = pd.to_datetime('now')
    df.to_sql("traderepublic", conn, index=False, if_exists='append')


if __name__ == "__main__":
    Fire(main)
