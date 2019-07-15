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


def get_da(soup):
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


def get_data(id):
    link = f"https://i.instagram.com/api/v1/users/{id}/info/"
    source = requests.get(link).text
    soup = BeautifulSoup(source, 'lxml')
    try:
        body = soup.body
        if body.class_=="p-error dialog-404":
            print('error!')
    finally:
        print('fn')
    text = json.loads(soup.text)
    username = text['user']['username']
    private = text['user']['is_private']
    verified = text['user']['is_verified']


get_data('10001485442')
