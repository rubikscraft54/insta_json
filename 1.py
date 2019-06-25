import os, time, threading, queue
import pandas as pd
from datetime import datetime


def hstg(str):
    raw_text = str[str.find('#'):]
    text = raw_text.split()
    for t in text:
        t = t.strip('\n')
        if not t.startswith('#'):
            text.remove(t)
        else:
            cnt = t.count('#')
            if cnt == 0:
                text.remove(t)
            if cnt == 1:
                pass
            else:
                t2 = t.lstrip('#')
                t2 = t2.split('#')
                for tt in t2:
                    text.append('#' + tt)
    yield text


def hstg2(t):
    for w in t:
        word = str(w)
        if word[0] != '#':
            t.remove(w)
            continue
        if word == '.':
            t.remove(w)
            continue
    yield t


def process(q, rows):
    while True:
        try:
            name = q.get()
            print(threading.currentThread().getName(), 'processing', os.path.basename(name), 'remaining: ', q.qsize())
            f = pd.read_csv(name)
            for i, row in f.iterrows():
                raw_text = str(row['text'])
                dt = datetime.fromtimestamp((row['timestamp']))
                dt = dt.strftime('%x')
                text = hstg2(hstg(raw_text))
                line = {'datetime': dt, 'short code': row['shortcode'], 'user id': row['user id'],
                        'likes': row['likes'], 'hashtags': text}
                rows.put(line)
            q.task_done()
        except queue.Empty:
            pass


def append(rows):
    global df
    df = pd.DataFrame(columns=['datetime', 'short code', 'user id', 'likes', 'hashtags'])
    while True:
        try:
            row = rows.get()
            if row['short code'] in df.loc[:, 'short code'].values:
                rows.task_done()
                continue
            df = df.append(row, ignore_index=True)
            rows.task_done()
        except queue.Empty:
            pass


def main():
    pth = '/Users/Sean/Documents/Intern/txt'
    files = queue.Queue()

    for f in os.listdir(pth):
        files.put(os.path.join(pth, f))
    rows = queue.Queue()

    for x in range(9):
        t = threading.Thread(target=process, args=(files, rows), daemon=True)
        t.start()
        time.sleep(.2)

    apt = threading.Thread(target=append, args=(rows, ), daemon=True)
    apt.start()
    timed_msg(rows)

    files.join()
    rows.join()


init_time = time.time()
main()
df.to_csv('/Users/Sean/Documents/Intern/analysis/dummy_test/result1.csv', header=True)
print(f'done in {round((time.time()-init_time)/60)}min')
