import pandas as pd
import os, time


#process text data and return list of hashtags
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


#merge csv files and filter data
def merge(path):
    os.chdir(path)
    df = pd.DataFrame(columns=['datetime', 'shortcode', 'user id', 'likes', 'text'])
    flist = [x for x in os.listdir(path)]
    num = len(flist)
    for f in flist:
        ti = time.time()
        csv = pd.read_csv(os.path.join(path, f))
        for i, row in csv.iterrows():
            raw_text = str(row['text'])
            if row['shortcode'] in df.loc[:, 'shortcode'].values:
                pass
            else:
                row['text'] = hstg(raw_text)
                df = df.append(row, ignore_index=True)
        num -= 1
        print(f'{os.path.basename(f)} Done | {os.path.getsize(os.path.join(path, f))/1000} KB | {num} remaining | took {time.time() - ti}s')
    df['datetime'] = pd.to_datetime(df['datetime'], unit='D')
    df.sort_values(by='datetime')
    df = df.set_index(['datetime'])
    df2 = df.loc['2018-01-01':'2019-12-31']
    return df, df2


#export to csv
def export(df, path):
    df.to_csv(path, header=True)


#create dataframe of unique hashtags by user
def hstg_byuser(df):
    userlist = []
    df.sort_values(by='user id')
    for i in df['user id']:
        if i in userlist:
            pass
        else:
            userlist.append(i)
    dict = {x:[] for x in userlist}

    for y in range(len(df)):
        dict[df.loc[y, 'user id']] += df.loc[y, 'hashtags']
    for z in userlist:
        dict[z] = list(set(dict[z]))

    resultdf = pd.DataFrame.from_dict(dict, orient='index', columns=['hashtags'])
    return resultdf


#unique posts by day
def posts_byday(df):
    dict = {}
    for i in df['datetime']:
        if i in dict:
            dict[i] += 1
        else:
            dict[i] = 1
    postcnt = pd.DataFrame.from_dict(dict, orient='index', columns=['posts'])
    return postcnt


#create dictionary of hashtag counts
def hstg_count(df):
    dict = {}
    for i in df['hashtags']:
        for h in i:
            if h in dict:
                dict[h] += 1
            else:
                dict[h] = 1
    cnt = pd.DataFrame.from_dict(dict, orient='index', columns=['count'])
    return cnt

def main():
    main_df, main_df1819 = merge(r'F:\txt')
    export(main_df, r'F:\analysis\MainDF_All.csv', header=True)
    export(main_df1819, r'F:\analysis\MainDF_1819.csv', header=True)

init_time = time.time()
main()
print("done in: ", time.time() - init_time)
