
import pandas as pd
import os, csv, json

#get list of directories to work with
def get_pathlist(dir):
    os.chdir(dir)
    pathlist = []

    for f in os.listdir(dir):
        pathlist.append(os.path.abspath(f))

    pathlist = list(dict.fromkeys(pathlist))
    return pathlist

#get list of files to work with
def get_fileslist(dir):
    os.chdir(dir)
    fileslist = []

    for f in os.listdir():
        if f.endswith('.json'):
            fileslist.append(f)
        else:
            pass

    fileslist = list(dict.fromkeys(fileslist))
    return fileslist

#get a list of hashtags, shortcode, owner id, date time and number of likes
def select_data(file):
    data = json.load(file)
    for x in data['node']['edge_media_to_caption']['edges']:
        text = x['node']['text']
        hstg = text.find('#')
        text = text[hstg:]
        global hstglist
        hstglist = text.split()
        for y in hstglist:
            if y.startswith('#'):
                pass
            else:
                hstglist.remove(y)


    timestamp = data['node']['taken_at_timestamp']
    like = data['node']['edge_liked_by']['count']
    scode = data['node']['shortcode']
    id = data['node']['owner']['id']
    return hstglist, timestamp, scode, id, like

#create dictionary of hashtag counts
def hstg_count(hstgs, empty_count):
    for h in hstgs:
        if h in empty_count:
            empty_count[h] += 1
        else:
            empty_count[h] = 1
    return empty_count

#append data to main DataFrame
def main_df(row, df):
    if row['shortcode'] in df.loc[:, 'shortcode'].values:
        pass
    else:
        df = df.append(row, ignore_index=True)
    return df

def main():
    count = {}
    df = pd.DataFrame(columns=['datetime', 'shortcode', 'user id', 'likes', 'hashtags'])
    pl = get_pathlist('/Users/Sean/Documents/Intern/Instagram-Json')
    for p in range(len(pl)):
        fileslist = get_fileslist(pl[p])
        for i in range(len(fileslist)):
            with open(fileslist[i]) as file:
                hstgs, dt, scode, id, like = select_data(file)
                row = {'datetime': dt, 'shortcode': scode, 'user id': id, 'likes':like, 'hashtags': hstgs}
                count = hstg_count(hstgs, count)
                df = main_df(row, df)
        print('finished processing folder ', pl[p])
    cnt = pd.DataFrame.from_dict(count, orient='index', columns=['count'])
    export_csv = df.to_csv(r'C:\Users\Sean\Documents\Intern\dataframe.txt', header=True)
    export_csv = cnt.to_csv(r'C:\Users\Sean\Documents\Intern\hstgcount.txt', header=True)

main()



