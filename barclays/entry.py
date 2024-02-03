from fire import Fire
import pandas as pd
import psycopg2
import logging
from sqlalchemy import create_engine
import numpy as np



def connect_to_postgres():
    conn = create_engine("postgresql://accounting:unicorn@127.0.0.1:5432/accounting")
    return conn


def convert_df(df: pd.DataFrame):
    # sadly this export needs some post processing
    # specifically, we need to drop a few rows at the top
    df_l = []
    for index, row in df.iterrows():
        if len(row) > 0 and row[0] == "Referenznummer":
            df_l.append(list(row))
            continue

        if len(df_l) > 0 and len(row) == 0:
            break

        if len(df_l) > 0 and len(row[1]) > 0:
            df_l.append(list(row))

    df_ret = pd.DataFrame(df_l[1:], columns=df_l[0])
    # convert the column names to lower case and replace spaces with underscores
    df_ret.columns = df_ret.columns.map(lambda x: x.lower().replace(' ', '_'))

    # dedupe column names
    s = df_ret.columns.to_series().groupby(df_ret.columns)
    df_ret.columns = np.where(s.transform('size')>1, 
                      df_ret.columns + s.cumcount().add(1).astype(str), 
                      df_ret.columns)

    # add current datetime to the dataframe
    df_ret['load_time'] = pd.to_datetime('now')
    
    return df_ret

def main(filename: str):
    logging.basicConfig(level=logging.INFO)
    
    conn = connect_to_postgres()
    df_raw = pd.read_excel(filename)
    df = convert_df(df_raw)
    df.to_sql("barclays", conn, index=False, if_exists='append')


if __name__ == "__main__":
    Fire(main)
