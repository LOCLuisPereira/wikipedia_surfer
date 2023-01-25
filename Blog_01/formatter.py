import sqlite3
db = sqlite3.connect('default.dp')

with open('edges.csv', 'w') as handler :
    print(f'start,end', file=handler)
    for idx, start, end in db.execute('select * from edges') :
        print(f'{start},{end}', file=handler)