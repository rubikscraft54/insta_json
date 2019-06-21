import pandas as pd
import os, json, time, threading, queue


def worker(q):
    while True:
        try:
            df = pd.DataFrame(columns=['timestamp', 'shortcode', 'user id', 'likes', 'text'])
            dt = str(q.get())
            print(threading.currentThread().getName(), 'processing', dt)
            fileslist = []
            folder = '/Users/Sean/Documents/Intern/Instagram-Json/#cutting'
            for f in os.listdir(folder):
                if f.startswith(dt) and f.endswith('.json'):
                    fileslist.append(os.path.join(folder, f))
                else:
                    pass
            for i in range(len(fileslist)):
                with open(fileslist[i]) as file:
                    data = json.load(file)
                    text = ''
                    for x in data['node']['edge_media_to_caption']['edges']:
                        text = x['node']['text']
                    timestamp = data['node']['taken_at_timestamp']
                    like = data['node']['edge_liked_by']['count']
                    scode = data['node']['shortcode']
                    id = data['node']['owner']['id']

                    row = {'timestamp': timestamp, 'shortcode': scode, 'user id': id, 'likes': like, 'text': text}
                    df = df.append(row, ignore_index=True)

            print('\t', threading.currentThread().getName(), 'finished ', dt)
            file_name = str(f'#cutting_{dt}.txt')
            path = r'C:/Users/Sean/Documents/Intern/txt/'
            df.to_csv(os.path.join(path, file_name), header=True)
            q.task_done()

        except queue.Empty:
            pass


def main():
    pl = ['2012', '2013', '2014-01', '2014-02', '2014-03', '2014-04', '2014-05', '2014-06',
          '2014-07', '2014-08', '2014-09', '2014-1', '2015-01', '2015-02', '2015-03', '2015-04',
          '2015-05', '2015-06', '2015-07', '2015-08', '2015-09', '2015-1', '2016-01', '2016-02',
          '2016-03', '2016-04', '2016-05', '2016-06', '2016-07', '2016-08', '2016-09', '2016-1',
          '2017-01', '2017-02', '2017-03', '2017-04', '2017-05', '2017-06', '2017-07', '2017-08',
          '2017-09', '2017-1', '2018-01', '2018-02', '2018-03', '2018-04', '2018-05', '2018-06',
          '2018-07', '2018-08', '2018-09', '2018-1', '2019-0']
    q = queue.Queue()
    for i in pl:
        q.put(i)

    threadList = []
    for x in range(10):
        t = threading.Thread(target=worker, args=(q, ), daemon=True)
        threadList.append(t)
        t.start()
        time.sleep(.5)

    q.join()


init_time = time.time()
main()
print("done in: ", time.time() - init_time)