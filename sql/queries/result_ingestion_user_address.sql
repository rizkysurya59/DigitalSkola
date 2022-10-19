SELECT COUNT(*), MAX(created_at)
FROM user_address_2018_snapshots
WHERE DATE(created_at) BETWEEN '2018-02-01' AND '2018-12-31';