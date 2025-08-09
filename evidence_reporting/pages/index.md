---
title: Welcome to Evidence
---

<Details title='How to edit this page'>

  This page can be found in your project at `/pages/index.md`. Make a change to the markdown file and save it to see the change take effect in your browser.
</Details>

```sql categories
  select distinct
      category
  from accounting.transactions_mapped
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
  group by 1, 2
```

<BarChart
    data={transactions_mapped}
    title="Transactions Mapped, {inputs.category.label}"
    x=month
    y=amount_eur
    series=category
/>

## What's Next?
- [Connect your data sources](settings)
- Edit/add markdown files in the `pages` folder
- Deploy your project with [Evidence Cloud](https://evidence.dev/cloud)

## Get Support
- Message us on [Slack](https://slack.evidence.dev/)
- Read the [Docs](https://docs.evidence.dev/)
- Open an issue on [Github](https://github.com/evidence-dev/evidence)
