import pymysql.cursors
import os, time, json
from datetime import datetime


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
        t = t.strip('\n')
        if t.count('#') > 1:
            t2 = t.lstrip('#')
            t2 = t2.split('#')
            for tt in t2:
                text.append('#' + tt)
        if t.count('#') == 1 and t.startswith('#'):
            text.append(t)
        else:
            continue
    t = ', '.join(text)
    return t


def worker(drt, db, cnt):
    folder_name = str(os.path.basename(drt))
    print('processing', folder_name)
    fileslist = []
    for f in os.listdir(drt):
        if f.endswith('.json'):
            fileslist.append(os.path.join(drt, f))
        else:
            pass
    for i in range(len(fileslist)):
        with open(fileslist[i]) as file:
            data = json.load(file)
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
        with db.cursor() as cursor:
            try:
                ins = "INSERT INTO `NSSI_JSONS` (`ID`, `NODE_ID`, `SHORTCODE`, `P_DATETIME`, `P_TIMESTAMP`, `LIKE_CNT`, `COMMENT_CNT`, `OWNER_ID`, `HASHTAGS`) VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(ins, (node_id, scode, dt, timestamp, like, cmnt, id, hstg))
            except pymysql.IntegrityError:
                pass
    cnt -= 1
    print('\t', 'finished ', folder_name, 'remaining: ', cnt)
    db.commit()


def main():
    db = pymysql.connect(host='35.227.104.92',
                         user='NSSI-user',
                         password='NSSI-password',
                         database='NSSI',
                         charset='utf8mb4',
                         cursorclass=pymysql.cursors.DictCursor)
    pl = get_pl('/Users/Sean/Documents/Intern/dbtest')
    for i in range(len(pl)):
        worker(pl[i], db, len(pl))
    db.close()


init_time = time.time()
main()
print("done in: ", time.time() - init_time)

"""
***Error msg:***


Traceback (most recent call last):
  File "C:/Users/Sean/Documents/Intern/py/db.py", line 83, in <module>
    main()
  File "C:/Users/Sean/Documents/Intern/py/db.py", line 78, in main
    worker(pl[i], db, len(pl))
  File "C:/Users/Sean/Documents/Intern/py/db.py", line 61, in worker
    cursor.execute(ins, (node_id, scode, dt, timestamp, like, cmnt, id, hstg))
  File "C:\Users\Sean\Documents\Intern\py\venv\lib\site-packages\pymysql\cursors.py", line 170, in execute
    result = self._query(query)
  File "C:\Users\Sean\Documents\Intern\py\venv\lib\site-packages\pymysql\cursors.py", line 328, in _query
    conn.query(q)
  File "C:\Users\Sean\Documents\Intern\py\venv\lib\site-packages\pymysql\connections.py", line 517, in query
    self._affected_rows = self._read_query_result(unbuffered=unbuffered)
  File "C:\Users\Sean\Documents\Intern\py\venv\lib\site-packages\pymysql\connections.py", line 732, in _read_query_result
    result.read()
  File "C:\Users\Sean\Documents\Intern\py\venv\lib\site-packages\pymysql\connections.py", line 1075, in read
    first_packet = self.connection._read_packet()
  File "C:\Users\Sean\Documents\Intern\py\venv\lib\site-packages\pymysql\connections.py", line 684, in _read_packet
    packet.check_error()
  File "C:\Users\Sean\Documents\Intern\py\venv\lib\site-packages\pymysql\protocol.py", line 220, in check_error
    err.raise_mysql_exception(self._data)
  File "C:\Users\Sean\Documents\Intern\py\venv\lib\site-packages\pymysql\err.py", line 109, in raise_mysql_exception
    raise errorclass(errno, errval)
pymysql.err.InternalError: (1366, "Incorrect string value: '\\xF0\\x9F\\x98\\x8D' for column 'HASHTAGS' at row 1")
"""
