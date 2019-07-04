import pymysql.cursors
import os, time, json, threading, queue, logging
from datetime import datetime


def conn():
    db = pymysql.connect(host='35.227.104.92',
                         user='NSSI-user',
                         password='NSSI-password',
                         database='NSSI',
                         charset='utf8mb4',
                         connect_timeout=200,
                         read_timeout=200,
                         write_timeout=200,
                         cursorclass=pymysql.cursors.DictCursor)
    return db


def get_pl(dr):
    os.chdir(dr)
    pl = []

    for f in os.listdir(dr):
        pl.append(os.path.abspath(f))

    pl = list(dict.fromkeys(pl))
    return pl


def fltr(tin):
    raw_text = tin[tin.find('#'):]
    text = []
    for t in raw_text.split():
        t = t.strip('\n\"')
        t = t.rstrip("\'.,")
        if t.count('#') > 1:
            t2 = t.lstrip('#')
            t2 = t2.split('#')
            for tt in t2:
                text.append('#' + tt)
        if t.count('#') == 1 and t.startswith('#'):
            text.append(t)
        else:
            continue
    t = ','.join(text)
    t.replace("'", "\'")
    return t


def worker(q):
    while True:
        try:
            drt = q.get_nowait()
            folder_name = str(os.path.basename(drt))
            print(f'*****{threading.currentThread().getName()} processing {os.path.basename(folder_name)}, {q.qsize()} remaining')
            files_list = []
            for f in os.listdir(drt):
                if f.endswith('.json'):
                    files_list.append(os.path.join(drt, f))
                else:
                    continue
            for i in range(len(files_list)):
                with open(files_list[i], encoding='utf8') as file:
                    data = json.load(file)
                    fnme2 = os.path.basename(files_list[i])
                    fnme = fnme2.rstrip('.json')
                    text = ''
                    for x in data['node']['edge_media_to_caption']['edges']:
                        text = x['node']['text']
                    hstg = fltr(text)
                    timestamp = data['node']['taken_at_timestamp']
                    node_id = data['node']['id']
                    like = data['node']['edge_liked_by']['count']
                    cmnt = data['node']['edge_media_to_comment']['count']
                    scode = data['node']['shortcode']
                    id = data['node']['owner']['id']
                    dt = datetime.date(datetime.fromtimestamp(timestamp))
                    if hstg == '' and cmnt >= 1:
                        try:
                            for c in data['node']['edge_media_to_comment']['edges']:
                                cid = c['node']['owner']['id']
                                if cid == id:
                                    text = text + c['node']['text']
                                else:
                                    continue
                        except KeyError:
                            pass
                        finally:
                            hstg = fltr(text)
                logger = logging.getLogger()
                db = conn()
                for v in range(3):
                    with db.cursor() as cursor:
                        try:
                            ins = "INSERT INTO `NSSI_JSONS` (`ID`, `NODE_ID`, `SHORTCODE`, `P_DATETIME`, `P_TIMESTAMP`, `LIKE_CNT`, `COMMENT_CNT`, `OWNER_ID`, `ROOT_HASHTAG`, `FILE_NAME`, `HASHTAGS`) VALUES (NULL, '%s', '%s', '%s', %s, %s, %s, '%s', \"%s\", '%s', \"%s\")" % \
                                  (node_id, scode, dt, timestamp, like, cmnt, id, folder_name, fnme, hstg)
                            cursor.execute(ins)
                            db.commit()
                        except pymysql.IntegrityError:
                            pass
                        except pymysql.err.ProgrammingError as detail:
                            logger.error(f'{folder_name}/{fnme}:{detail}')
                            pass
                        except (pymysql.err.InternalError, pymysql.err.OperationalError) as detail:
                            logger.error(detail)
                            db.close()
                            db = conn()
                            continue
                        except (ConnectionError, TimeoutError) as detail:
                            logger.error(detail)
                            db.close()
                            db = conn()
                            continue
                        finally:
                            db.close()
                            break
            print(f'{threading.currentThread().getName()} finished {os.path.basename(folder_name)}, {q.qsize()} remaining')
            q.task_done()

        except queue.Empty:
            pass


def main():
    pl = get_pl(r'C:\Users\Sean\Documents\Intern\Instagram-Json')
    q = queue.Queue()
    for p in pl:
        q.put(p)

    for i in range(11):
        t = threading.Thread(target=worker, args=(q, ), daemon=True)
        t.start()
        time.sleep(0.1)
    q.join()


init_time = time.time()
main()
print("done in: ", time.time() - init_time)
