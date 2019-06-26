import os, time, threading, queue
import pandas as pd
from datetime import datetime


def hstg(tin):
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
    return text


def process(q, rows):
    while True:
        try:
            name = q.get()
            print(f'{threading.currentThread().getName()} processing {os.path.basename(name)}, {q.qsize()} remaining')
            f = pd.read_csv(name)
            for i, row in f.iterrows():
                raw_text = str(row['text'])
                dt = datetime.date(datetime.fromtimestamp(row['timestamp']))
                line = (row['shortcode'], [dt, row['shortcode'], row['user id'],
                        row['likes'], hstg(raw_text)])
                rows.put(line)
            q.task_done()
        except queue.Empty:
            pass


def append(rows):
    while True:
        try:
            line = rows.get()
            df_dict[line[0]] = line[1]
            rows.task_done()
        except queue.Empty:
            pass


def main():
    pth = '/Users/Sean/Documents/Intern/txt'
    files = queue.Queue()
    global df_dict
    df_dict = {}

    for f in os.listdir(pth):
        files.put(os.path.join(pth, f))
    rows = queue.Queue()

    for x in range(5):
        t = threading.Thread(target=process, args=(files, rows), daemon=True)
        t.start()
        time.sleep(.1)

    for x in range(6):
        t = threading.Thread(target=append, args=(rows, ), daemon=True)
        t.start()
        time.sleep(.1)

    files.join()
    rows.join()
    df = pd.DataFrame.from_dict(df_dict, orient='index',
                                columns=['datetime', 'short code', 'user id', 'likes', 'hashtags'])
    df = df.set_index('datetime')
    df.to_csv('/Users/Sean/Documents/Intern/analysis/result.txt', header=True)


init_time = time.time()
main()
print(f'done in {round((time.time()-init_time)/60)}min')

