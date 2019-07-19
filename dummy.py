from bs4 import BeautifulSoup
import pymysql.cursors
import pandas as pd
import time, threading, requests, json


def conn():
    db = pymysql.connect(host='localhost',
                         user='root',
                         db='idsc',
                         password='Since2018!',
                         charset='utf8mb4',
                         cursorclass=pymysql.cursors.DictCursor)
    return db


def get_data(id):
    link = f"https://i.instagram.com/api/v1/users/{id}/info/"
    source = requests.get(link).text
    soup = BeautifulSoup(source, 'lxml')
    user = {}
    try:
        text = json.loads(soup.text)
        user["num_id"] = id
        user["username"] = text['user']['username']
        user["private"] = text['user']['is_private']
        user["verified"] = text['user']['is_verified']
        user["media_cnt"] = text['user']['media_count']
        user["followers"] = text['user']['follower_count']
        user["following"] = text['user']['following_count']
        user["bio"] = text['user']['biography']
        user["igtv"] = text['user']['total_igtv_videos']
        user["effects"] = text['user']['total_ar_effects']
        user["usertags"] = text['user']['usertags_count']
        user["interest"] = text['user']['is_interest_account']
        user["hlreels"] = text['user']['has_highlight_reels']
        user["report"] = text['user']['can_be_reported_as_fraud']
        user["business"] = text['user']['is_potential_business']
        user["auto"] = text['user']['auto_expand_chaining']
        user["hldis"] = text['user']['highlight_reshare_disabled']
        user["ftags"] = text['user']['following_tag_count']
        enum = ["auto", "report", "hlreels", "hldis", "interest", "private", "verified", "business"]
        for x in enum:
            if user[x] is True:
                user[x] = 'Y'
            if user[x] is False:
                user[x] = 'N'
    except json.decoder.JSONDecodeError:
        print('user not found')
    print(user)
    return user

get_data('10001485442')
