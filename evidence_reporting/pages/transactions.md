# Transactions overview

## All

```sql transactions
  select
     transaction_date
     , account
     , amount_eur
     , category
     , description
  from accounting.transactions_mapped
  where transaction_date >= date_trunc('year', current_date) - interval '1 year'
  order by transaction_date desc
```

<DataTable data={transactions}>
	<Column id=transaction_date />
	<Column id=account />
	<Column id=amount_eur fmt=eur2 />
    <Column id=category />
    <Column id=description />
</DataTable>

## Unmapped (if any)

```sql unmapped_transactions
  select
    transaction_date
     , account
     , amount_eur
     , description
  from accounting.transactions_mapped
  where transaction_date >= date_trunc('year', current_date) - interval '1 year'
  and category is null
```

<DataTable data={unmapped_transactions}>
	<Column id=transaction_date />
	<Column id=account />
	<Column id=amount_eur fmt=eur2 />
    <Column id=description />
</DataTable>