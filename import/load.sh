export PGPASSWORD=unicorn
export LEDGER_FILE=~/Cloud/ledger/stefan.ledger

psql -h 127.0.0.1 -U accounting -d accounting \
                    -c "create table if not exists postings(id serial,txnidx int,date1 date,date2 date,status text,code text,description text,comment text,account text,amount numeric,commodity text,credit numeric,debit numeric,posting_status text,posting_comment text, load_date timestamp NOT NULL DEFAULT NOW());"

hledger print -O sql | psql -h 127.0.0.1 -U accounting -d accounting
