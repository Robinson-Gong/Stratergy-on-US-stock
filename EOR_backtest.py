# -*- coding: utf-8 -*-
"""
Created on Thu Nov 10 12:39:38 2022

@author: yiyu
"""

import pandas as pd
# #%%
# df1 = pd.read_csv('D:\\File\\Study\\Hedge Fund\\stock_prices_latest.csv')
# #%%
# df2 = pd.read_csv('D:\\File\\Study\\Hedge Fund\\earnings_latest.csv')
# #%%
# df1.loc[:,'date'] = pd.to_datetime(df1.loc[:,'date'],format='%Y-%m-%d')
# #%%
# df2.loc[:,'date'] = pd.to_datetime(df2.loc[:,'date'],format='%Y-%m-%d')
# #%%
# df1['earning'] = 0
# #%%
# df2 = df2.dropna(axis=0,how='all')
# #%%
# df1 = df1.fillna(method='bfill')
# #%%
# data = pd.merge(left = df1,right = df2, how='outer',on=['symbol','date'])
# #%%
# import datetime
# #%%
# time_sp = datetime.datetime(2009,5,1)
# #%%
# data = data[data['date']>=time_sp]
# #%%
# data.sort_values(by = ['symbol','date'], inplace = True)
# #%%
# data.reset_index(inplace = True,drop=True)
# #%%
# import gc
# del df1
# del df2
# gc.collect
# #%%
# temp = data.iloc[:,10:14].isnull()
# #%%
# result1 = temp[temp['qtr']==False].index.tolist()
# result2 = temp[temp['eps_est']==False].index.tolist()
# result3 = temp[temp['eps']==False].index.tolist()
# result4 = temp[temp['release_time']==False].index.tolist()
# #%%
# result = result1+result2+result3+result4
# #%%
# result = list(set(result))
# #%%
# result = [i+1 for i in result]
# #%%
# a = data['earning']
# #%%
# a.loc[result] = 1
# #%%
# data['earning'] = a
# #%%
# data = data[['symbol','earning','close_adjusted','date']]
# #%%
# data['ret'] = data['close_adjusted'].pct_change()
# #%%
# data['year'] = data['date'].apply(lambda x:x.year)
# data['month'] = data['date'].apply(lambda x:x.month)
# data['day'] = data['date'].apply(lambda x:x.day)
# #%%
# data.to_parquet(r'D:\\File\\Study\\Hedge Fund\\stock_data.parquet.gzip')
#%%
df = pd.read_parquet(r'D:\\File\\Study\\Hedge Fund\\stock_data.parquet.gzip')
#%%
stock_pool = df[df['earning']==1]
#%%
stock_long_pool = stock_pool[stock_pool['ret']>=0.07]
stock_short_pool = stock_pool[stock_pool['ret']<=-0.07]
#%%
stock_long_pool['year'] = stock_long_pool['date'].apply(lambda x:x.year)
stock_short_pool['year'] = stock_short_pool['date'].apply(lambda x:x.year)
#%%
stock_long_pool['month'] = stock_long_pool['date'].apply(lambda x:x.month)
stock_short_pool['month'] = stock_short_pool['date'].apply(lambda x:x.month)
#%%
from datetime import datetime
#%%
import tushare as ts
ts.set_token('0a112a7348164981d00b71036039b5dbe17ad91a9e7e3dce97c9b6ec')
#%%
pro = ts.pro_api()
df1 = pro.index_global(ts_code='SPX', start_date='20090731', end_date='20210131')
#%%
df1['trade_date'] = pd.to_datetime(df1['trade_date'],format = '%Y%m%d')
#%%
df1.sort_values(by='trade_date',inplace=True)
#%%
df1.reset_index(inplace=True,drop=True)
#%%
df1['year'] = df1['trade_date'].apply(lambda x:x.year)
df1['month'] = df1['trade_date'].apply(lambda x:x.month)
#%%
def getportfolio(long,short,year,num,price,short_ratio,upper_bound,lower_bound):
    l_t= long[long['year']== year]
    l_p = long[long['year']== year-1]
    l_p = l_p[l_p['month']==12]
    l_p['month'] = -12
    l = pd.concat([l_p,l_t])
    s_t= short[short['year']== year]
    s_p = short[short['year']== year-1]
    s_p = s_p[s_p['month']==12]
    s_p['month'] = -12
    s = pd.concat([s_p,s_t])
    # p1_l = pd.concat([l[l['month']==1],l[l['month']==-12]]).sort_values(by='ret',ascending=False)
    # p1_l = p1_l[p1_l['ret']<=0.15]
    # p1_l = p1_l[p1_l['close_adjusted']>=price].head(num)
    # p1_l['wt'] = 1/num
    # p1_l['direction'] = 'long'
    # p1_s = pd.concat([s[s['month']==1],s[s['month']==-12]]).sort_values(by='ret')
    # p1_s = p1_s[p1_s['ret']>=-0.25]
    # r = int(p1_l.shape[0]*short_ratio)
    # p1_s = p1_s[p1_s['close_adjusted']>=price].head(r)
    # p1_s['wt'] = 1/num
    # p1_s['direction'] = 'short'
    # p1 = pd.concat([p1_l,p1_s])
    # p1 = p1_l
    
    
    p1_l = pd.concat([l[l['month']==1],l[l['month']==-12]]).sort_values(by='ret',ascending=False)
    p1_s = pd.concat([s[s['month']==1],s[s['month']==-12]]).sort_values(by='ret')
    p1_l = p1_l[p1_l['ret']<=upper_bound]
    p1_s = p1_s[p1_s['ret']>=lower_bound]
    p1_l['total_mv'] = 'nan'
    p1_s['total_mv'] = 'nan'
    if p1_l.shape[0]<p1_s.shape[0]:
        p1_s = p1_s[p1_s['close_adjusted']>=price].head(num)
        r = int(p1_s.shape[0]*p1_l.shape[0]/p1_s.shape[0])
        p1_l = p1_l[p1_l['close_adjusted']>=price].head(r)
    else:
        if p1_l.shape[0] != 0:
            p1_l = p1_l[p1_l['close_adjusted']>=price].head(num)
            r = int(p1_l.shape[0]*p1_s.shape[0]/p1_l.shape[0])
            p1_s = p1_s[p1_s['close_adjusted']>=price].head(r)
    p1_l['wt'] = 1/num
    p1_l['direction'] = 'long'
    p1_s['wt'] = 1/num
    p1_s['direction'] = 'short'
    # p1 = pd.concat([p1_l,p1_s])
    p1=p1_l
    
    del p1_l
    del p1_s
    
    # p4_l = pd.concat([l[l['month']==3],l[l['month']==4]]).sort_values(by='ret',ascending=False)
    # p4_l = p4_l[p4_l['ret']<=0.15]
    # p4_l = p4_l[p4_l['close_adjusted']>=price].head(num)
    # p4_l['wt'] = 1/num
    # p4_s = pd.concat([s[s['month']==3],s[s['month']==4]]).sort_values(by='ret')
    # p4_s = p4_s[p4_s['ret']>=-0.25]
    # r = int(p4_l.shape[0]*short_ratio)
    # p4_s = p4_s[p4_s['close_adjusted']>=price].head(r)
    # p4_s['wt'] = 1/num
    # p4_l['direction'] = 'long'
    # p4_s['direction'] = 'short'
    # p4 = pd.concat([p4_l,p4_s])
    
    p4_l = pd.concat([l[l['month']==3],l[l['month']==4]]).sort_values(by='ret',ascending=False)
    p4_s = pd.concat([s[s['month']==3],s[s['month']==4]]).sort_values(by='ret')
    p4_l = p4_l[p4_l['ret']<=upper_bound]
    p4_s = p4_s[p4_s['ret']>=lower_bound]
    if p4_l.shape[0]<p4_s.shape[0]:
        p4_s = p4_s[p4_s['close_adjusted']>=price].head(num)
        r = int(p4_s.shape[0]*p4_l.shape[0]/p4_s.shape[0])
        p4_l = p4_l[p4_l['close_adjusted']>=price].head(r)
    else:
        if p4_l.shape[0] != 0:
            p4_l = p4_l[p4_l['close_adjusted']>=price].head(num)
            r = int(p4_l.shape[0]*p4_s.shape[0]/p4_l.shape[0])
            p4_s = p4_s[p4_s['close_adjusted']>=price].head(r)
    p4_l['wt'] = 1/num
    p4_l['direction'] = 'long'
    p4_s['wt'] = 1/num
    p4_s['direction'] = 'short'
    # p4 = pd.concat([p4_l,p4_s])
    p4=p4_l
    
    # p4= p4_l
    del p4_l
    del p4_s
    # p7_l = pd.concat([l[l['month']==6],l[l['month']==7]]).sort_values(by='ret',ascending=False)
    # p7_l = p7_l[p7_l['ret']<=0.15]
    # p7_l = p7_l[p7_l['close_adjusted']>=price].head(num)
    # p7_l['wt'] = 1/num
    # p7_s = pd.concat([s[s['month']==6],s[s['month']==7]]).sort_values(by='ret')
    # p7_s = p7_s[p7_s['ret']>=-0.25]
    # r = int(p7_l.shape[0]*short_ratio)
    # p7_s = p7_s[p7_s['close_adjusted']>=price].head(r)
    # p7_s['wt'] = 1/num
    # p7_l['direction'] = 'long'
    # p7_s['direction'] = 'short'
    # p7 = pd.concat([p7_l,p7_s])
    #p7= p7_l
    
    p7_l = pd.concat([l[l['month']==6],l[l['month']==7]]).sort_values(by='ret',ascending=False)
    p7_s = pd.concat([s[s['month']==6],s[s['month']==7]]).sort_values(by='ret')
    p7_l = p7_l[p7_l['ret']<=upper_bound]
    p7_s = p7_s[p7_s['ret']>=lower_bound]
    if p7_l.shape[0]<p7_s.shape[0]:
        p7_s = p7_s[p7_s['close_adjusted']>=price].head(num)
        r = int(p7_s.shape[0]*p7_l.shape[0]/p7_s.shape[0])
        p7_l = p7_l[p7_l['close_adjusted']>=price].head(r)
    else:
        p7_l = p7_l[p7_l['close_adjusted']>=price].head(num)
        r = int(p7_l.shape[0]*p7_s.shape[0]/p7_l.shape[0])
        p7_s = p7_s[p7_s['close_adjusted']>=price].head(r)
    p7_l['wt'] = 1/num
    p7_l['direction'] = 'long'
    p7_s['wt'] = 1/num
    p7_s['direction'] = 'short'
    # p7 = pd.concat([p7_l,p7_s])
    p7=p7_l
    
    del p7_l
    del p7_s
    # p8_l = pd.concat([l[l['month']==7],l[l['month']==8]]).sort_values(by='ret',ascending=False)
    # p8_l = p8_l[p8_l['ret']<=0.15]
    # p8_l = p8_l[p8_l['close_adjusted']>=price].head(num)
    # p8_l['wt'] = 1/num
    # p8_s = pd.concat([s[s['month']==7],s[s['month']==8]]).sort_values(by='ret')
    # p8_s = p8_s[p8_s['ret']>=-0.25]
    # r = int(p8_l.shape[0]*short_ratio)
    # p8_s = p8_s[p8_s['close_adjusted']>=price].head(r)
    # p8_s['wt'] = 1/num
    # p8_l['direction'] = 'long'
    # p8_s['direction'] = 'short'
    # p8 = pd.concat([p8_l,p8_s])
    #p8 = p8_l
    
    p8_l = pd.concat([l[l['month']==7],l[l['month']==8]]).sort_values(by='ret',ascending=False)
    p8_s = pd.concat([s[s['month']==7],s[s['month']==8]]).sort_values(by='ret')
    p8_l = p8_l[p8_l['ret']<=upper_bound]
    p8_s = p8_s[p8_s['ret']>=lower_bound]
    
    if p8_l.shape[0]<p8_s.shape[0]:
        p8_s = p8_s[p8_s['close_adjusted']>=price].head(num)
        r = int(p8_s.shape[0]*p8_l.shape[0]/p8_s.shape[0])
        p8_l = p8_l[p8_l['close_adjusted']>=price].head(r)
    else:
        p8_l = p8_l[p8_l['close_adjusted']>=price].head(num)
        r = int(p8_l.shape[0]*p8_s.shape[0]/p8_l.shape[0])
        p8_s = p8_s[p8_s['close_adjusted']>=price].head(r)
    p8_l['wt'] = 1/num
    p8_l['direction'] = 'long'
    p8_s['wt'] = 1/num
    p8_s['direction'] = 'short'
    # p8 = pd.concat([p8_l,p8_s])
    p8=p8_l
    
    del p8_l
    del p8_s
    # p10_l = pd.concat([l[l['month']==9],l[l['month']==10]]).sort_values(by='ret',ascending=False)
    # p10_l = p10_l[p10_l['ret']<=0.15]
    # p10_l = p10_l[p10_l['close_adjusted']>=price].head(num)
    # p10_l['wt'] = 1/num
    # p10_s = pd.concat([s[s['month']==9],s[s['month']==10]]).sort_values(by='ret')
    # p10_s = p10_s[p10_s['ret']>=-0.25]
    # r = int(p10_l.shape[0]*short_ratio)
    # p10_s = p10_s[p10_s['close_adjusted']>=price].head(r)
    # p10_s['wt'] = 1/num
    # p10_l['direction'] = 'long'
    # p10_s['direction'] = 'short'
    # p10 = pd.concat([p10_l,p10_s])
    # p10 = p10_l
    
    p10_l = pd.concat([l[l['month']==9],l[l['month']==10]]).sort_values(by='ret',ascending=False)
    p10_s = pd.concat([s[s['month']==9],s[s['month']==10]]).sort_values(by='ret')
    p10_l = p10_l[p10_l['ret']<=upper_bound]
    p10_s = p10_s[p10_s['ret']>=lower_bound]
    if p10_l.shape[0]<p10_s.shape[0]:
        p10_s = p10_s[p10_s['close_adjusted']>=price].head(num)
        r = int(p10_s.shape[0]*p10_l.shape[0]/p10_s.shape[0])
        p10_l = p10_l[p10_l['close_adjusted']>=price].head(r)
    else:
        p10_l = p10_l[p10_l['close_adjusted']>=price].head(num)
        r = int(p10_l.shape[0]*p10_s.shape[0]/p10_l.shape[0])
        p10_s = p10_s[p10_s['close_adjusted']>=price].head(r)
    p10_l['wt'] = 1/num
    p10_l['direction'] = 'long'
    p10_s['wt'] = 1/num
    p10_s['direction'] = 'short'
    # p10 = pd.concat([p10_l,p10_s])
    p10 = p10_l
    
    return p1,p4,p7,p8,p10
#%%
def back_track(year):
    data = df[df['year']==year]
    p1,p4,p7,p8,p10 = getportfolio(stock_long_pool, stock_short_pool, year, 50,10,0.5,0.15,-0.15)
    season1 = pd.DataFrame(columns = ['symbol','earning','close_adjusted','date','ret','year','month','day','wt','direction'])
    season2 = pd.DataFrame(columns = ['symbol','earning','close_adjusted','date','ret','year','month','day','wt','direction'])
    season3 = pd.DataFrame(columns = ['symbol','earning','close_adjusted','date','ret','year','month','day','wt','direction'])
    season4 = pd.DataFrame(columns = ['symbol','earning','close_adjusted','date','ret','year','month','day','wt','direction'])
    season5 = pd.DataFrame(columns = ['symbol','earning','close_adjusted','date','ret','year','month','day','wt','direction'])
    for i in p1.index.tolist():
        if p1.dropna().empty == True:
            break
        temp = data[(data.symbol == p1.loc[i,'symbol'])]
        temp1 = temp[(temp.month == 1)]
        temp1 = temp1.iloc[[-1]]
        temp1['wt'] = p1.loc[i,'wt']
        temp1['direction'] = p1.loc[i,'direction']
        temp2 = temp[(temp.month == 4)]
        if temp2.dropna().empty == True:
            break
        temp2 = temp2.iloc[[-1]]
        season1 = pd.concat([season1,temp1])
        season1 = pd.concat([season1,temp2])
    for i in p4.index.tolist():
        if p4.dropna().empty == True:
            break
        temp = data[(data.symbol == p4.loc[i,'symbol'])]
        temp1 = temp[(temp.month == 4)]
        temp1 = temp1.iloc[[-1]]
        temp1['wt'] = p4.loc[i,'wt']
        temp1['direction'] = p4.loc[i,'direction']
        temp2 = temp[(temp.month == 7)]
        if temp2.dropna().empty == True:
            break
        temp2 = temp2.iloc[[-1]]
        season2 = pd.concat([season2,temp1])
        season2 = pd.concat([season2,temp2])
    for i in p7.index.tolist():
        if p7.dropna().empty == True:
            break
        temp = data[(data.symbol == p7.loc[i,'symbol'])]
        temp1 = temp[(temp.month == 7)]
        temp1 = temp1.iloc[[-1]]
        temp1['wt'] = p7.loc[i,'wt']
        temp1['direction'] = p7.loc[i,'direction']
        temp2 = temp[(temp.month == 8)]
        if temp2.dropna().empty == True:
            break
        temp2 = temp2.iloc[[-1]]
        season3 = pd.concat([season3,temp1])
        season3 = pd.concat([season3,temp2])
    for i in p8.index.tolist():
        if p8.dropna().empty == True:
            break
        temp = data[(data.symbol == p8.loc[i,'symbol'])]
        temp1 = temp[(temp.month == 8)]
        temp1 = temp1.iloc[[-1]]
        temp1['wt'] = p8.loc[i,'wt']
        temp1['direction'] = p8.loc[i,'direction']
        temp2 = temp[(temp.month == 10)]
        if temp2.dropna().empty == True:
            break
        temp2 = temp2.iloc[[-1]]
        season4 = pd.concat([season4,temp1])
        season4 = pd.concat([season4,temp2])
    for i in p10.index.tolist():
        if p10.dropna().empty == True:
            break
        temp = data[(data.symbol == p10.loc[i,'symbol'])]
        temp1 = temp[(temp.month == 10)]
        temp1 = temp1.iloc[[-1]]
        temp1['wt'] = p10.loc[i,'wt']
        temp1['direction'] = p10.loc[i,'direction']
        temp2 = df[(df.symbol == p10.loc[i,'symbol'])&(df.year==year+1)]
        temp2 = temp2[(temp2.month == 1)]
        if temp2.dropna().empty == True:
            break
        temp2 = temp2.iloc[[-1]]
        season5 = pd.concat([season5,temp1])
        season5 = pd.concat([season5,temp2])
    return season1,season2,season3,season4,season5
#%%
result = pd.DataFrame()
for i in range(2009,2021,1):
    season1,season2,season3,season4,season5 = back_track(i)
    for j in range(int(season1.shape[0]/2)):
        if season1.dropna().empty:
            break
        else:
            season1.reset_index(drop = True,inplace = True)
        if season1.loc[2*j,'direction'] == 'long':
            season1.loc[2*j,'period_ret'] = ((season1.loc[2*j+1,'close_adjusted']/season1.loc[2*j,'close_adjusted']) - 1)*100*season1.loc[2*j,'wt']
        else:
            season1.loc[2*j,'period_ret'] = -((season1.loc[2*j+1,'close_adjusted']/season1.loc[2*j,'close_adjusted']) - 1)*100*season1.loc[2*j,'wt']
    if not season1.dropna().empty:
        sum_ret = season1['period_ret'].sum()
        temp = {'year' :[i],'period': ['1'],'period_ret':[sum_ret]}
        temp = pd.DataFrame(data=temp)
        result = pd.concat([result,temp])
    for j in range(int(season2.shape[0]/2)):
        if season2.dropna().empty:
            break
        else:
            season2.reset_index(drop = True,inplace = True)
        if season2.loc[2*j,'direction'] == 'long':
            season2.loc[2*j,'period_ret'] = ((season2.loc[2*j+1,'close_adjusted']/season2.loc[2*j,'close_adjusted']) - 1)*100*season2.loc[2*j,'wt']
        else:
            season2.loc[2*j,'period_ret'] = -((season2.loc[2*j+1,'close_adjusted']/season2.loc[2*j,'close_adjusted']) - 1)*100*season2.loc[2*j,'wt']
    if not season2.dropna().empty:
        sum_ret = season2['period_ret'].sum()
        temp = {'year' :[i],'period': ['2'],'period_ret':[sum_ret]}
        temp = pd.DataFrame(data=temp)
        result = pd.concat([result,temp])
    for j in range(int(season3.shape[0]/2)):
        if season3.dropna().empty:
            break
        else:
            season3.reset_index(drop = True,inplace = True)
        if season3.loc[2*j,'direction'] == 'long':
            season3.loc[2*j,'period_ret'] = ((season3.loc[2*j+1,'close_adjusted']/season3.loc[2*j,'close_adjusted']) - 1)*100*season3.loc[2*j,'wt']
        else:
            season3.loc[2*j,'period_ret'] = -((season3.loc[2*j+1,'close_adjusted']/season3.loc[2*j,'close_adjusted']) - 1)*100*season3.loc[2*j,'wt']
    if not season3.dropna().empty:
        sum_ret = season3['period_ret'].sum()
        temp = {'year' :[i],'period': ['3'],'period_ret':[sum_ret]}
        temp = pd.DataFrame(data=temp)
        result = pd.concat([result,temp])
    for j in range(int(season4.shape[0]/2)):
        if season4.dropna().empty:
            break
        else:
            season4.reset_index(drop = True,inplace = True)
        if season4.loc[2*j,'direction'] == 'long':
            season4.loc[2*j,'period_ret'] = ((season4.loc[2*j+1,'close_adjusted']/season4.loc[2*j,'close_adjusted']) - 1)*100*season4.loc[2*j,'wt']
        else:
            season4.loc[2*j,'period_ret'] = -((season4.loc[2*j+1,'close_adjusted']/season4.loc[2*j,'close_adjusted']) - 1)*100*season4.loc[2*j,'wt']
    if not season4.dropna().empty:
        sum_ret = season4['period_ret'].sum()
        temp = {'year' :[i],'period': ['4'],'period_ret':[sum_ret]}
        temp = pd.DataFrame(data=temp)
        result = pd.concat([result,temp])
    for j in range(int(season5.shape[0]/2)):
        if season5.dropna().empty:
            break
        else:
            season5.reset_index(drop = True,inplace = True)
        if season5.loc[2*j,'direction'] == 'long':
            season5.loc[2*j,'period_ret'] = ((season5.loc[2*j+1,'close_adjusted']/season5.loc[2*j,'close_adjusted']) - 1)*100*season5.loc[2*j,'wt']
        else:
            season5.loc[2*j,'period_ret'] = -((season5.loc[2*j+1,'close_adjusted']/season5.loc[2*j,'close_adjusted']) - 1)*100*season5.loc[2*j,'wt']
    if not season5.dropna().empty:
        sum_ret = season5['period_ret'].sum()
        temp = {'year' :[i],'period': ['5'],'period_ret':[sum_ret]}
        temp = pd.DataFrame(data=temp)
        result = pd.concat([result,temp])
    season1.to_csv(r'D:\\File\\Study\\Hedge Fund\\M1_'+str(i)+'.csv',index=False)
    season2.to_csv(r'D:\\File\\Study\\Hedge Fund\\M4_'+str(i)+'.csv',index=False)
    season3.to_csv(r'D:\\File\\Study\\Hedge Fund\\M7_'+str(i)+'.csv',index=False)
    season4.to_csv(r'D:\\File\\Study\\Hedge Fund\\M8_'+str(i)+'.csv',index=False)
    season5.to_csv(r'D:\\File\\Study\\Hedge Fund\\M10_'+str(i)+'.csv',index=False)
#%%
# result = pd.DataFrame()
# i=2010
# season1,season2,season3,season4,season5 = back_track(i)
# for j in range(int(season1.shape[0]/2)):
#     if not season1.dropna().empty:
#         season1.reset_index(drop = True,inplace = True)
#         if season1.loc[2*j,'direction'] == 'long':
#             season1.loc[2*j,'period_ret'] = ((season1.loc[2*j+1,'close_adjusted']/season1.loc[2*j,'close_adjusted']) - 1)*100*season1.loc[2*j,'wt']
#         else:
#             season1.loc[2*j,'period_ret'] = -((season1.loc[2*j+1,'close_adjusted']/season1.loc[2*j,'close_adjusted']) - 1)*100*season1.loc[2*j,'wt']
# if not season1.dropna().empty:
#     sum_ret = season1['period_ret'].sum()
#     temp = {'year' :[i],'period': ['1'],'period_ret':[sum_ret]}
#     temp = pd.DataFrame(data=temp)
#     result = pd.concat([result,temp])
# for j in range(int(season2.shape[0]/2)):
#     if not season2.dropna().empty:
#         season2.reset_index(drop = True,inplace = True)
#         if season2.loc[2*j,'direction'] == 'long':
#             season2.loc[2*j,'period_ret'] = ((season2.loc[2*j+1,'close_adjusted']/season2.loc[2*j,'close_adjusted']) - 1)*100*season2.loc[2*j,'wt']
#         else:
#             season2.loc[2*j,'period_ret'] = -((season2.loc[2*j+1,'close_adjusted']/season2.loc[2*j,'close_adjusted']) - 1)*100*season2.loc[2*j,'wt']
# if not season2.dropna().empty:
#     sum_ret = season2['period_ret'].sum()
#     temp = {'year' :[i],'period': ['2'],'period_ret':[sum_ret]}
#     temp = pd.DataFrame(data=temp)
#     result = pd.concat([result,temp])
# for j in range(int(season3.shape[0]/2)):
#     if not season3.dropna().empty:
#         season3.reset_index(drop = True,inplace = True)
#         if season3.loc[2*j,'direction'] == 'long':
#             season3.loc[2*j,'period_ret'] = ((season3.loc[2*j+1,'close_adjusted']/season3.loc[2*j,'close_adjusted']) - 1)*100*season3.loc[2*j,'wt']
#         else:
#             season3.loc[2*j,'period_ret'] = -((season3.loc[2*j+1,'close_adjusted']/season3.loc[2*j,'close_adjusted']) - 1)*100*season3.loc[2*j,'wt']
# if not season3.dropna().empty:
#     sum_ret = season3['period_ret'].sum()
#     temp = {'year' :[i],'period': ['3'],'period_ret':[sum_ret]}
#     temp = pd.DataFrame(data=temp)
#     result = pd.concat([result,temp])
# for j in range(int(season4.shape[0]/2)):
#     if not season4.dropna().empty:
#         season4.reset_index(drop = True,inplace = True)
#         if season4.loc[2*j,'direction'] == 'long':
#             season4.loc[2*j,'period_ret'] = ((season4.loc[2*j+1,'close_adjusted']/season4.loc[2*j,'close_adjusted']) - 1)*100*season4.loc[2*j,'wt']
#         else:
#             season4.loc[2*j,'period_ret'] = -((season4.loc[2*j+1,'close_adjusted']/season4.loc[2*j,'close_adjusted']) - 1)*100*season4.loc[2*j,'wt']
# if not season4.dropna().empty:
#     sum_ret = season4['period_ret'].sum()
#     temp = {'year' :[i],'period': ['4'],'period_ret':[sum_ret]}
#     temp = pd.DataFrame(data=temp)
#     result = pd.concat([result,temp])
# for j in range(int(season5.shape[0]/2)):
#     if not season5.dropna().empty:
#         season5.reset_index(drop = True,inplace = True)
#         if season5.loc[2*j,'direction'] == 'long':
#             season5.loc[2*j,'period_ret'] = ((season5.loc[2*j+1,'close_adjusted']/season5.loc[2*j,'close_adjusted']) - 1)*100*season5.loc[2*j,'wt']
#         else:
#             season5.loc[2*j,'period_ret'] = -((season5.loc[2*j+1,'close_adjusted']/season5.loc[2*j,'close_adjusted']) - 1)*100*season5.loc[2*j,'wt']
# if not season5.dropna().empty:
#     sum_ret = season5['period_ret'].sum()
#     temp = {'year' :[i],'period': ['5'],'period_ret':[sum_ret]}
#     temp = pd.DataFrame(data=temp)
#     result = pd.concat([result,temp])
#%%
result = pd.DataFrame()
for i in range(2009,2021,1):
    season1=pd.read_csv(r'D:\\File\\Study\\Hedge Fund\\M1_'+str(i)+'.csv')
    season2=pd.read_csv(r'D:\\File\\Study\\Hedge Fund\\M4_'+str(i)+'.csv')
    season3=pd.read_csv(r'D:\\File\\Study\\Hedge Fund\\M7_'+str(i)+'.csv')
    season4=pd.read_csv(r'D:\\File\\Study\\Hedge Fund\\M8_'+str(i)+'.csv')
    season5=pd.read_csv(r'D:\\File\\Study\\Hedge Fund\\M10_'+str(i)+'.csv')
    if not season1.dropna().empty:
        sum_ret = season1['period_ret'].sum()
        temp = {'year' :[i],'period': ['1'],'period_ret':[sum_ret]}
        temp = pd.DataFrame(data=temp)
        result = pd.concat([result,temp])

    if not season2.dropna().empty:
        sum_ret = season2['period_ret'].sum()
        temp = {'year' :[i],'period': ['2'],'period_ret':[sum_ret]}
        temp = pd.DataFrame(data=temp)
        result = pd.concat([result,temp])

    if not season3.dropna().empty:
        sum_ret = season3['period_ret'].sum()
        temp = {'year' :[i],'period': ['3'],'period_ret':[sum_ret]}
        temp = pd.DataFrame(data=temp)
        result = pd.concat([result,temp])

    if not season4.dropna().empty:
        sum_ret = season4['period_ret'].sum()
        temp = {'year' :[i],'period': ['4'],'period_ret':[sum_ret]}
        temp = pd.DataFrame(data=temp)
        result = pd.concat([result,temp])
    if not season5.dropna().empty:
        sum_ret = season5['period_ret'].sum()
        temp = {'year' :[i],'period': ['5'],'period_ret':[sum_ret]}
        temp = pd.DataFrame(data=temp)
        result = pd.concat([result,temp])
#%%
result.reset_index(inplace=True,drop=True)
#%%
cumsum = 1
for i in range(58):
    cumsum = cumsum*(1+result.loc[i,'period_ret']/100)
    result.loc[i,'cumsum'] = cumsum
#%%
l = [1,4,7,8,10]
spx = pd.DataFrame()
for i in range(2009,2022,1):
    for j in l:
        temp = df1[df1['year']==i]
        temp = temp[temp['month']==j]
        if not temp.empty:
            temp = temp.iloc[[-1]]
            spx = pd.concat([spx,temp])
#%%
spx.reset_index(inplace = True, drop= True)
#%%
cumsum=1
for i in range(58):
    spx.loc[i,'period_ret'] = spx.loc[i+1,'close']/spx.loc[i,'close'] - 1
    cumsum = cumsum*(1+spx.loc[i,'period_ret'])
    spx.loc[i,'cumsum']=cumsum
#%%
result['spx_ret'] = spx['period_ret'].dropna()
result['spx_cumsum'] = spx['cumsum'].dropna()
result['period_ret'] = result['period_ret']/100
#%%
import matplotlib.pyplot as plt
#%%
result['alpha'] = result['cumsum'] - result['spx_cumsum']
#%%
alpha = pd.DataFrame()
#%%
alpha['date'] = spx['trade_date']
#%%
alpha['portfolio_ret'] = 1
alpha['spx_ret'] = 1
#%%
for i in range(58):
    alpha.loc[i+1,'portfolio_ret'] = result.loc[i,'cumsum']
    alpha.loc[i+1,'spx_ret'] = result.loc[i,'spx_cumsum']
#%%
alpha['portfolio_ret'] = 100*alpha['portfolio_ret']
alpha['spx_ret'] = 100*alpha['spx_ret']
#%%
alpha['alpha'] = alpha['portfolio_ret']-alpha['spx_ret']
#%%
ax = plt.figure(figsize=(10,5))
plt.plot(alpha['date'],alpha['portfolio_ret'],label='portfolio_ret')
plt.plot(alpha['date'],alpha['spx_ret'],color = 'red',label='sp500_ret')
plt.plot(alpha['date'],alpha['alpha'],color = 'blue',label = 'alpha')
plt.grid()
plt.legend()
plt.show()
#%%
sharpe = (result['period_ret'].mean() - result['spx_ret'].mean())/((result['period_ret'] - result['spx_ret']).std())
#%%
import numpy as np

def MaxDrawdown(return_list):
    '''最大回撤率'''
    i = np.argmax((np.maximum.accumulate(return_list) - return_list) / np.maximum.accumulate(return_list))  # 结束位置
    if i == 0:
        return 0
    j = np.argmax(return_list[:i])  # 开始位置
    return (return_list[j] - return_list[i]) / (return_list[j])
#%%
return_list = alpha['portfolio_ret']
mdd = MaxDrawdown(return_list)
print(mdd)
#%%
result.to_csv(r'D:\\File\\Study\\Hedge Fund\\result.csv')
#%%
period_ret = pd.DataFrame(columns = ['year','ret'])
j=0
for i in range(2009,2021,1):
    temp = result[result['year']==i]
    period_ret.loc[j,'year'] = i
    period_ret.loc[j,'ret'] = temp.iloc[-1,3]/temp.iloc[0,3] - 1
    j = j+1
#%%
period_ret.to_csv(r'D:\\File\\Study\\Hedge Fund\\period_ret.csv')
#%%
import math
#%%
average_anual_ret = math.pow(result.iloc[-1,3],1/12) - 1
#%%
annual = pd.DataFrame()
annual['year'] = result['year'].unique()
#%%
def getret(df):
    df = df.reset_index(drop= True)
    cum = 1
    for i in range(df.shape[0]):
        cum = cum*(1+df.loc[i,'period_ret'])
    ret = cum-1
    return ret
#%%
for i in range(annual.shape[0]):
    temp = result[result['year']==annual.loc[i,'year']] 
    annual.loc[i,'ret'] = getret(temp)
#%%
sharpe = (annual['ret'].mean() - 0.0419)/annual['ret'].std()