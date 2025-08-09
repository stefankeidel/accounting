---
title: Expenses
---

```sql categories
  select distinct
      category
  from accounting.transactions_mapped
  where transaction_date >= date_trunc('year', current_date) - interval '1 year'
  and category ilike 'Expenses:%'
  order by 1
```

<Dropdown data={categories} name=category value=category>
    <DropdownOption value="%" valueLabel="All Categories"/>
</Dropdown>


```sql transactions_mapped
  select
      date_trunc('month', transaction_date) as month,
      category,
      sum(amount_eur) as amount_eur
  from accounting.transactions_mapped
  where category like '${inputs.category.value}'
  and category ilike 'Expenses:%'
  and transaction_date >= date_trunc('year', current_date) - interval '1 year'
  group by 1, 2
```

<BarChart
    data={transactions_mapped}
    title="Transactions Mapped, {inputs.category.label}"
    x=month
    y=amount_eur
    series=category
/>
