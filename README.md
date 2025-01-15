# My Accounting rig

This repository contains all code related to my accounting rig. It's
based around a Postgres Database running on one of my servers.

The services called `gls/`, `barclays/` etc. are simple scripts that
export data from the banks and services I use and put them into the
Postgres as they are.

I then use [dbt-core](https://github.com/dbt-labs/dbt-core) to
transform that data into something I can use for budgeting and
analyses to keep on top of my finances. That's the `dbt/` directory.

It's not particularly polished and it's not automatically orchestrated
yet. I run the individual scripts by hand because they're usually
based on some CSV I have to download from the bank's website.

Since I have no new data coming in automatically, there's no need to
run `dbt` automatically either. So I just execute `dbt build` whenever
I have new data. For now, at least. If I ever end up looking for a job
I might polish this all a bit to showcase how I think a data stack
could work :)
