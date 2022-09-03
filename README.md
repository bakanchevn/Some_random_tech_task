
# Tasks assumptions  
I assume that query looks like 
```sql
SELECT
  t.transaction_category_id,
  SUM(t.transaction_amount) AS sum_amount,
  COUNT(DISTINCT t.user_id) AS num_users
FROM transactions t
JOIN users u USING (user_id)
WHERE t.is_blocked = False
  AND u.is_active = True
GROUP BY t.transaction_category_id
ORDER BY sum_amount DESC;
```
Instead of ```is_active = 1``` because in csv generation ```is_active``` is ```True``` or ```False```

Also i assume that ```transaction_amount``` is ```Decimal``` in the ```transactions``` table  instead of ```Integer```

- [Task 1](Task1.md)
- [Task 2](Task2.md)
- [Task 3](Task3.md)
