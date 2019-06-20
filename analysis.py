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
    for f in os.listdir(path):
        num = 88
        csv = pd.read_csv(f)
        for row in csv.iterrows():
            raw_text = row['text']
            if row['shortcode'] in df.loc[:, 'shortcode'].values:
                pass
            else:
                row['text'] = hstg(raw_text)
                df = df.append(row, ignore_index=True)
        num -= 1
        print(f'Finished {os.path.basename(f)}, {num} files remaining')
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
        for h in i[h]:
            if h in dict:
                dict[h] += 1
            else:
                dict[h] = 1
    cnt = pd.DataFrame.from_dict(dict, orient='index', columns=['count'])
    return cnt


def main():
    merge(r'C:\Users\Sean\Documents\Intern\txt')
    df = opendf('C:\Users\Sean\Documents\Intern\dataframetest.txt')
    df1819 = df_1819(df)

    #export unique hashtag counts 2018-2019
    hcnt_1819 = hstg_count(df1819)
    export_csv = hcnt_1819.to_csv(r'C:\Users\Sean\Documents\Intern\hstgcount_1819.txt', header=True)

    #export unique posts by day
    pbd_1819 = posts_byday(df1819)
    export_csv = pbd_1819.to_csv(r'C:\Users\Sean\Documents\Intern\posts_byday_1819.txt', header=True)
    pbd = posts_byday(df)
    export_csv = pbd.to_csv(r'C:\Users\Sean\Documents\Intern\posts_byday.txt', header=True)

    #export unique hashtags by user
    h_byuser = hstg_byuser(df)
    export_csv = h_byuser.to_csv(r'C:\Users\Sean\Documents\Intern\h_byuser.txt', header=True)


init_time = time.time()
main()
print("done in: ", time.time() - init_time)







