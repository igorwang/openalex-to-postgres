# openalex-to-postgres

Python scripts for convert openalex dataset to postgresql database.

## Usuage

- Download openalex from AWS3
- Flatten jsonl data to csv
  - flatten-openalex-works-to-csv.py
  - flatten-openalex-other-jsonl.py
  - Tips: The data related to works are very large so it will takes a lot of time to parse. The speed is also depends on your hardware of computers.
- Build a postgresql database
  - Use docker-compose with postgresql-single
  - Or use your own instance
  - create database that name is 'openalex'
- Use openalex-pg-schema.sql to create schema
- Use import_csv_to_postgresql.py import csv to db

## Result

- schema

![zbNXgJ](https://pic.techower.com/uPic/zbNXgJ.png)

- data preview

![3j05LP](https://pic.techower.com/uPic/3j05LP.png)

## Dumps ans Restore for PG

- dumps

```
pg_dump -h 127.0.0.1 -p 45432 -U admin -d postgres -F d -f dump_dir -j 10
```

- restore

```
pg_restore -h 127.0.0.1 -p 45432 -U admin -d postgres -F d dump_dir -j 10
```

If you need the dumped format, please contact us.

## Reference

- https://github.com/ourresearch/openalex-documentation-scripts

