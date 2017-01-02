

Must implement:

  * `/SAVE`
  * `/GET`
  * `/CHANGED_SINCE` 

and keep track of dates.

```sql
SELECT row_to_json(r) AS record, 'table1' AS table FROM (SELECT * FROM table1 WHERE last_update > %(timestamp)
  UNION ALL
SELECT row_to_json(r) AS record, 'table2' AS table FROM (SELECT * FROM table2 WHERE last_update > %(timestamp)
  UNION ALL
SELECT row_to_json(r) AS record, 'table3' AS table FROM (SELECT * FROM table3 WHERE last_update > %(timestamp)
```
