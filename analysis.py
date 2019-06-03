import pandas as pd

#open csv file, convert to dataframe and change datetime format
def opendf(path):
    df = pd.read_csv(path)
    df['datetime'] = pd.to_datetime(df['datetime'], unit='D')
    df.sort_values(by='datetime')
    return df

#create second dataframe with posts dated 2018-2019
def df_1819(df):
    df = df.set_index(['datetime'])
    df = df.loc['2018-01-01':'2019-12-31']
    export_csv = df.to_csv(r'C:\Users\Sean\Documents\Intern\dataframetest18-19.txt', header=True)
    return df

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

main()







