from bs4 import BeautifulSoup
import pymysql.cursors
import pandas as pd
import time, threading, requests, json


def conn():
    db = pymysql.connect(host='localhost',
                         user='root',
                         db='idsc',
                         password='Since2018!',
                         cursorclass=pymysql.cursors.DictCursor)
    return db


def get_data(soup):
    script = soup.find('script', type="application/ld+json")
    txt = json.loads(script.text)
    name = txt['author']['alternateName'].lstrip('@')
    link = 'https://www.instagram.com/p/' + name
    return name


def check(sc):
    link = 'https://www.instagram.com/p/' + sc
    source = requests.get(link).text
    soup = BeautifulSoup(source, 'lxml')
    body = soup.body
    print(body)
    return soup


def main():
    file = pd.read_csv(r'C:\Users\Sean\Documents\Intern\id\id-shortcode.csv')
    for i, line in file.iterrows():
        id = str(line['OWNER_ID'])
        sc = str(line['SHORTCODE'])
        pcnt = int(line['POST_CNT'])
        cnx = conn()
        with cnx.cursor() as crs:
            ins = "INSERT INTO `owner_ID` (`NUM_ID`, `Shortcode`, `Post_cnt`) VALUES ('%s', '%s', '%s')" % (id, sc, pcnt)
            crs.execute(ins)
            cnx.commit()
        cnx.close()
        print(f'line {i} inserted')


init_time = time.time()
print(f'took {(time.time()-init_time)/60} mins')
