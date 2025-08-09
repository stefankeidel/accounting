# High level Summary Dashboard

## Networth Current (a.k.a. Account Balances)

```sql networth_current
  select *
  from networth_current
  order by networth asc
```

<DataTable data={networth_current}/>

## Networth Monthly

```sql networth_monthly
  select *
  from networth_monthly
  order by 1 desc
```

<LineChart
    data={networth_monthly}
    x=month
    y=networth
    yAxisTitle="Net Worth per Month"
    yFmt="eur"
/>

<DataTable data={networth_monthly}/>

## Networth Yearly

```sql networth_yearly
  select *
  from networth_yearly
  order by 1 desc
```

<BarChart
  data={networth_yearly}
  x=year
  y=networth
  y2=networth_delta
  y2SeriesType="line"
  yFmt="eur"
/>