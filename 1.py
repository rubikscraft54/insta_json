import os, time, threading, queue
import pandas as pd


def hstg(str):
    raw_text = str[str.find('#'):]
    text = raw_text.split()
    for t in text:
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
    return text


def process(q, rows):
    name = q.get()
    f = pd.read_csv(name)
    for i, row in f.iterrows():
        raw_text = str(row['text'])
        row['text'] = hstg(raw_text)
        row['timestamp'] = pd.to_datetime(row['timestamp'], unit='D')
        rows.put(row)
    q.taskdone()


def append(rows):
    df = pd.DataFrame(columns=['timestamp', 'shortcode', 'user id', 'likes', 'text'])
    scode = []
    while True:
        try:
            row = rows.get()
            if not row['shortcode'] in scode:
                scode.append(row['shortcode'])
                df = df.append(row, ignore_index=True)
                rows.taskdone()

        except rows.empty:
            if stopFlag == 1:
                df.sort_values(by='datetime')
                df = df.set_index(['datetime'])
                df2 = df.loc['2018-01-01':'2019-12-31']
                df.to_csv(path, header=True)
                df.to_csv(path, header=True)
                break



def main():
    pth = '/Users/Sean/Documents/Intern/txt'
    files = queue.Queue()
    global stopFlag
    stopFlag = 0

    for f in os.listdir(pth):
        files.put(os.path.join(pth, f))
    rows = queue.Queue()

    for x in range(9):
        t = threading.Thread(target=process, args=(files, rows), daemon=True)
        t.start()
        time.sleep(.3)

    apt = threading.Thread(target=append, args=(rows, ))
    apt.start()

    files.join()
    rows.join()

    if files.empty and rows.empty:
        stopFlag = 1


init_time = time.time()
main()
print(f'done in {(time.time()-init_time)/60}min')

