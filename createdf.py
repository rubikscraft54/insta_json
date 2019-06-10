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
def folder_df(q, df):
    folder = q.get(True, 1)
    print(threading.currentThread().getName(), 'processing', folder)
    fileslist = []
    for f in os.listdir(folder):
        if f.endswith('.json'):
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

    print(threading.currentThread().getName(), 'finished processing folder ', folder)
    fname = str(os.path.basename(folder) + '.txt')
    path = r'C:/Users/Sean/Documents/Intern/txt/'
    df.to_csv(os.path.join(path, fname), header=True)
    q.task_done()

def main():
    df = pd.DataFrame(columns=['timestamp', 'shortcode', 'user id', 'likes', 'text'])
    pl = get_pathlist('/Users/Sean/Documents/Intern/Instagram-Json')
    wq = queue.Queue()
    threadList = []
    for i in pl:
        wq.put(i)
    for x in range(8):
        t = threading.Thread(target=folder_df, args=(wq, df))
        t.setDaemon(True)
        threadList.append(t)
        t.start()
        time.sleep(2)
    wq.join()

    for t in threadList:
        t.join()



init_time = time.time()
main()
print("done in: ", time.time() - init_time)