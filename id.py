from bs4 import BeautifulSoup
import pymysql.cursors
import time, threading, requests, json


#setup database connection
def conn():
    db = pymysql.connect(host='',
                         user='',
                         db='',
                         password='',
                         charset='utf8mb4',
                         cursorclass=pymysql.cursors.DictCursor)
    return db


#changes True False boolean to string Y/N
def tf(input):
    if input is True:
        return 'Y'
    if input is False:
        return 'N'


#worker function: receive chunks and iterate rows
def get_data(rows):
    size = len(rows)
    db = conn()
    for i in range(size):
        id = rows.pop(0)['NUM_ID']
        link = f"https://i.instagram.com/api/v1/users/{id}/info/"
        source = requests.get(link).text
        soup = BeautifulSoup(source, 'lxml')
        #creates loop for request rate limit block
        while True:
            try:
                text = json.loads(soup.text)
                usr = text['user']['username']
                prv = tf(text['user']['is_private'])
                vrf = tf(text['user']['is_verified'])
                mcnt = text['user']['media_count']
                flwrs = text['user']['follower_count']
                flwng = text['user']['following_count']
                bio = text['user']['biography']
                igtv = text['user']['total_igtv_videos']
                efct = text['user']['total_ar_effects']
                usrtgs = text['user']['usertags_count']
                intrst = tf(text['user']['is_interest_account'])
                hlrls = tf(text['user']['has_highlight_reels'])
                rpt = tf(text['user']['can_be_reported_as_fraud'])
                bsns = tf(text['user']['is_potential_business'])
                auto = tf(text['user']['auto_expand_chaining'])
                hldis = tf(text['user']['highlight_reshare_disabled'])
                ftgs = text['user']['following_tag_count']

                try:
                    with db.cursor() as crs:
                        ins = "INSERT INTO `users` (`num_id`,`username`,`private`,`verified`,`media_count`,`followers`," \
                              "`following`,`biography`,`igtv_videos`,`total_ar_effects`,`usertags`,`interest_account`," \
                              "`highlight_reels`,`can_be_reported`,`potential_business`,`auto_expand_chaining`," \
                              "`highlight_reshare_disabled`,`ftags`) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', " \
                              "\"%s\", '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (id, usr, prv,
                              vrf, mcnt, flwrs, flwng, bio, igtv, efct, usrtgs, intrst, hlrls, rpt, bsns, auto, hldis, ftgs)
                        crs.execute(ins)
                    break
                    
                #handles syntax error caused by escape characters
                except pymysql.err.ProgrammingError:
                    print(id)
                    with db.cursor() as crs:
                        ins = "INSERT INTO `users` (`num_id`,`username`,`private`,`verified`,`media_count`,`followers`," \
                              "`following`,`biography`,`igtv_videos`,`total_ar_effects`,`usertags`,`interest_account`," \
                              "`highlight_reels`,`can_be_reported`,`potential_business`,`auto_expand_chaining`," \
                              "`highlight_reshare_disabled`,`ftags`) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', " \
                              "NULL, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (id, usr, prv,
                              vrf, mcnt, flwrs, flwng, igtv, efct, usrtgs, intrst, hlrls, rpt, bsns, auto, hldis, ftgs)
                        crs.execute(ins)
                    break
            #saves deleted/inacative account id in a separate database
            except json.decoder.JSONDecodeError:
                with db.cursor() as crs:
                    ins = "INSERT INTO `inactive` (`NUM_ID`) VALUES ('%s')" % id
                    crs.execute(ins)
                    break
            #waits 60 seconds when blocked      
            except KeyError:
                print(threading.currentThread().getName(), 'waiting 60 seconds')
                time.sleep(60)
        db.commit()
    db.close()


def main():
    db = conn()
    #receive number ids from table owner_id
    with db.cursor() as crs:
        crs.execute("SELECT NUM_ID FROM owner_id")
        rows1 = crs.fetchmany(size=50000)
        rows2 = crs.fetchmany(size=50000)
        rows3 = crs.fetchmany(size=50000)
        rows4 = crs.fetchmany(size=50000)
        rows5 = crs.fetchmany(size=50000)
        rows6 = crs.fetchmany(size=50000)
        rows7 = crs.fetchmany(size=50000)
        rows8 = crs.fetchmany(size=50000)
        rows9 = crs.fetchmany(size=50000)
        rows10 = crs.fetchall()
    db.close()

    t1 = threading.Thread(target=get_data, args=(rows1,), daemon=True)
    t2 = threading.Thread(target=get_data, args=(rows2,), daemon=True)
    t3 = threading.Thread(target=get_data, args=(rows3,), daemon=True)
    t4 = threading.Thread(target=get_data, args=(rows4,), daemon=True)
    t5 = threading.Thread(target=get_data, args=(rows5,), daemon=True)
    t6 = threading.Thread(target=get_data, args=(rows6,), daemon=True)
    t7 = threading.Thread(target=get_data, args=(rows7,), daemon=True)
    t8 = threading.Thread(target=get_data, args=(rows8,), daemon=True)
    t9 = threading.Thread(target=get_data, args=(rows9,), daemon=True)
    t10 = threading.Thread(target=get_data, args=(rows10,), daemon=True)

    tl = [t1, t2, t3, t4, t5, t6, t7, t8, t9, t10]
    for t in tl:
        t.start()
    for t in tl:
        t.join()


init_time = time.time()
main()
print(f'done in {round((time.time()-init_time)/60)}min')
