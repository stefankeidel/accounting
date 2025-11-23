import pandas as pd
from sqlalchemy import create_engine
from fire import Fire
import logging

logging.basicConfig(level=logging.INFO)


def connect_to_postgres():
    conn = create_engine("postgresql://postgres:unicorn@nixie:5432/accounting")
    return conn


def main(account_name: str, output_file: str):
    conn = connect_to_postgres()

    export_query = f"""
    select transaction_id, source, transaction_date, account, amount_eur, category, description
    from analytics.analysis__transactions_with_categories
    where account ilike '{account_name}'
    order by transaction_date desc, transaction_id asc
    """

    df = pd.read_sql(export_query, conn)

    logging.info(f"Exported {len(df)} rows for account {account_name}")

    # truncate category column which has values like Expenses:Rent to just 'Rent'
    df["category"] = df["category"].str.split(":").str[-1]

    df.to_csv(output_file, index=False)


if __name__ == "__main__":
    Fire(main)
