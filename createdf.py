import pandas as pd
import os, json, time, threading, queue


#get list of directories to work with
def get_pathlist(dir):
    os.chdir(dir)
    pathlist = []

    for f in os.listdir(dir):
        pathlist.append(os.path.abspath(f))

    pathlist = list(dict.fromkeys(pathlist))
    return pathlist


#create a single df for a folder and export as txt
def worker(q):
    while True:
        try:
            drt = q.get_nowait()
            folder_name = str(os.path.basename(drt))
            df = pd.DataFrame(columns=['timestamp', 'shortcode', 'user id', 'likes', 'text'])
            print(threading.currentThread().getName(), 'processing', folder_name)
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
                    timestamp = data['node']['taken_at_timestamp']
                    like = data['node']['edge_liked_by']['count']
                    scode = data['node']['shortcode']
                    id = data['node']['owner']['id']

                    row = {'timestamp': timestamp, 'shortcode': scode, 'user id': id, 'likes': like, 'text': text}
                    df = df.append(row, ignore_index=True)

            print('\t', threading.currentThread().getName(), 'finished ', folder_name, 'remaining: ', q.qsize())
            file_name = str(os.path.basename(drt) + '.txt')
            path = r'C:/Users/Sean/Documents/Intern/txt/'
            df.to_csv(os.path.join(path, file_name), header=True)
            q.task_done()

        except queue.Empty:
            pass


def main():
    plx = get_pathlist('/Users/Sean/Documents/Intern/Instagram-Json')
    pl = ['C:/Users/Sean/Documents/Intern/Instagram-Json/#selfharmmm',
          'C:/Users/Sean/Documents/Intern/Instagram-Json/#selfharm',
          'C:/Users/Sean/Documents/Intern/Instagram-Json/#cutting']
    q = queue.Queue()
    for i in pl:
        q.put(i)

    threadList = []
    for x in range(4):
        t = threading.Thread(target=worker, args=(q,), daemon=True)
        threadList.append(t)
        t.start()
        time.sleep(1)

    q.join()


init_time = time.time()
main()
print("done in: ", time.time() - init_time)
