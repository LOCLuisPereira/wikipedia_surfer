''' PARAMENTERS '''
DEFAULT_MAX = 20000
CLEAN = False
START = [
    'Portugal'
]


''' IMPORTS '''
from keybert import KeyBERT
from redis import Redis
from time import sleep
import wikipediaapi
import sqlite3
import spacy
import os


''' KEYS AND MODELS '''
wiki_wiki = wikipediaapi.Wikipedia(
        language='en',
        extract_format=wikipediaapi.ExtractFormat.WIKI
)
nlp = spacy.load("en_core_web_sm")
kw_model = KeyBERT()


def extract_links(page):
    xs = []
    links = page.links
    for title in sorted(links.keys()):
        # print(title, '-', links[title])
        xs.append(title)
    return xs


''' DATABASES '''
mem = Redis(db=4, decode_responses=True)
db = sqlite3.connect('default.dp')


''' FIRST TIME CLEANING '''
if CLEAN :
    db.execute('drop table if exists edges')

    for k in mem.keys() :
        mem.delete( k )

    for k in START :
        mem.lpush('__mem__', k)

    db.execute('''
        create table if not exists edges(
            edge_id integer primary key,
            start text not null,
            end text not null
        )
    ''')

STAT = sum([1 for _ in mem.keys()]) - 1

while True :

    if STAT > DEFAULT_MAX : break

    xterm = mem.rpop('__mem__')
    
    if not xterm :
        print('> Ending')
        break
    
    if mem.get(xterm) :
        print(f'> Already done: {xterm}')
        continue


    STAT += 1
    print(f'> {STAT}: {xterm}')

    ''' Try to mine. If something happens, restart the process'''
    try :
        page = wiki_wiki.page(xterm)

        if not page.exists() : continue

        mem.set(xterm, 1)

        links = extract_links( page )

        xs = []
        for l in links :
            try :
                l = l.replace('\'','')
                query = f'''insert into edges (start,end) values ('{xterm}', '{l}')'''
                db.execute(query)
                db.commit()
                mem.lpush('__mem__', l)
            except Exception as e:
                print(query)

    except Exception as e:
        print(e)
        mem.rpush('__mem__', xterm)
        break