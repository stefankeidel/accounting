# Income Overview (Unrealised gains/losses notwithstanding)

```sql transactions_mapped
select
  date_trunc('month', transaction_date) as month,
  category,
  sum(amount_eur) as total_income
from transactions_mapped
where category ilike 'Income:%'
and category <> 'Income:Capital Gains'
group by 1,2
```

<BarChart
    data={transactions_mapped}
    x="month"
    y="total_income"
    series=category
/>

# Yearly Zoom Out

```sql transactions_mapped_yearly
select
  date_trunc('year', transaction_date) as year,
  category,
  sum(amount_eur) as total_income
from transactions_mapped
where category ilike 'Income:%'
and category <> 'Income:Capital Gains'
group by 1,2
order by 1 desc
```

<BarChart
    data={transactions_mapped_yearly}
    y="total_income"
    x="year"
    series="category"
  />