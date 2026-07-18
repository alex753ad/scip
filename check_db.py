import sqlite3, os

db = 'history.db'
if not os.path.exists(db):
    print('history.db не найден')
else:
    con = sqlite3.connect(db)
    print('--- Записи bounce/breakout ---')
    print(con.execute("SELECT COUNT(*), MIN(created_at), MAX(created_at) FROM level_outcomes WHERE outcome IN ('bounce','breakout')").fetchone())
    print('--- Распределение исходов ---')
    for r in con.execute('SELECT outcome, COUNT(*) FROM level_outcomes GROUP BY outcome'):
        print(r)
    print('--- monitoring_age_minutes > 0 ---')
    print(con.execute('SELECT COUNT(*) FROM level_outcomes WHERE monitoring_age_minutes > 0').fetchone())
    con.close()
