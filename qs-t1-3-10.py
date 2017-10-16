#encoding:UTF-8
import datetime 
import time
import requests #__version__ = 2.3.0 这里直接使用session，因为要先登陆 
from bs4 import BeautifulSoup #__version__ = 4.3.2
from lxml import etree
import re
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
s=requests.session()
from pymongo import MongoClient,ASCENDING
mc=MongoClient("localhost",27017)
from pandas import Series,DataFrame
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
def sf():
    global fs
    medi={
        '凤凰财经': 1.2,
        '搜狐财经': 1.2,
        '腾讯财经': 1.2,
        '网易财经': 1.2,
        '新浪财经': 1.2,

        '凤凰理财首页': 1,
        '搜狐理财': 1,
        '腾讯房产': 1,
        '腾讯理财': 1,
        '网易房产': 1,
        '网易理财': 1,
        '新浪理财': 1,

        '人民网--金融': 1,
        '人民网--汽车': 1,
        '人民网--房产': 1,
        '新华网--房产': 1,
        '新华网--汽车': 1,
        '中新网--产经': 1,
        '中新网--金融': 1,
        '中新网--汽车': 1,
        '人民网--财经': 1,
        '新华网--财经': 1,
        '中国网--财经': 1,
        '中新网--财经': 1,

        '财经网': 1,
        '财新网': 1,
        '东方财富网': 1,
        '中金在线': 1,
        '和讯首页': 1,
        '金融界财经': 1,
        '我爱卡': 1,
        '房天下': 1,

        '财新网--房产公司': 0.8,
        '财经网--地产': 0.8,
        '财经网--金融': 0.8,
        '财新网--公司': 0.8,
        '财新网--金融': 0.8,
        '财新网--经济': 0.8,
        '东方财富网--财经': 0.8,
        '东方财富网--理财': 0.8,
        '东方财富网--汽车': 0.8,
        '东方财富网--银行': 0.8,
        '中金在线--财经': 0.8,
        '中金在线--房产': 0.8,
        '中金在线--理财': 0.8,
        '中金在线--汽车': 0.8,
        '和讯理财': 0.8,
        '和讯房产': 0.8,
        '和讯新闻': 0.8,
        '和讯银行': 0.8,
        '金融界理财': 0.8,
        '金融界首页': 0.8,
        '金融界优车': 0.8,
        '金融界优房': 0.8,
        '我爱卡贷款': 0.8,
        '我爱卡理财': 0.8
    }

    stander=medi.get(str(r[8]),0)
    print stander

    if r[1] ==u"True":
        k=5
    else:
        k=0
   
    #【p】出现次数计分
    if r[3]==0:
        p=0
    elif r[3]==1:
        p=1
    elif r[3]==2:
        p=1.5
    elif r[3]==3:
        p=1.8
    else:
        p=2
    #【ts】时长计分
    if r[9]<=480:
        ts=r[9]*0.5/60
    elif r[9]<=720:
        ts=4 + (r[9] - 480) * 0.375 / 60
    elif r[9]<960:
        ts=5.5 + (r[9] - 720) * 0.25 / 60
    else:
        ts=6.5
    #print ts
    #print p
    #print k
    fs=stander*(ts+p+k)
    #print fs
    #return fs


db=mc.monitor360
wb = Workbook()
ws1 = wb.active
ws1.title = u'融360品牌监控'

for x in range(1001,1006,1):
    print x
    #print u'shishenm'

#from multiprocessing.dummy import Pool as ThreadPool
#建立link为索引。ASCENDING=升序   DESCENDING=降序
#db.test1.create_index([("link",ASCENDING)])
    st=str(x)+"-0000"
    et=str(x)+"-2400"
    #st=str(x)+"-0000"
    #et=str(x)+"-2400"
    #提取数
    c =db.test2.find(
        {
        "$and":[
               {"spidertime":
            {
                   "$elemMatch":{"$gte":st,"$lte":et}
            }},
           # {u"正文":re.compile(u'房贷')},
            #{
            #{"$and":[
            #    {u"正文":re.compile(u'房贷')},
            #    {u"正文":re.compile(u'利率')}
                #{u"正文":re.compile(u'理财')},
                #{u"正文":re.compile(u'数据')}

                #{u"正文":re.compile(u'宝宝')},
                #{u"正文":re.compile(u'数据')}
                #{u"正文":re.compile(u'存款')},
                #{u"正文":re.compile(u'利率')}
                #{u"正文":re.compile(u'网贷')},
                #]},
               
            #},
            {"$or":[
                    {"融360":{"$gte":1}},
                    {"网贷之家":{"$gte":1}},
                    {"网贷天眼":{"$gte":1}},
                    #{"$or":[
                    #    {"网贷之家":{"$gte":1}},
                     #   {"网贷天眼":{"$gte":1}}
                      #  ]
                    #},
                    {"盈灿咨询":{"$gte":1}},
                    {"零壹财经":{"$gte":1}}
                    ]}
            ]})

    # 写入EXCEL第一行作为字段名

    ws1.append(['爬取时间', '标题是否提及融360', '文章标题', '融360', '文章链接', 
            '文章推荐发布时间', '来源', '类型','推荐位置', '推荐时长(分钟)','得分','关键词','计分日','是否为理财文章'])
    df2=pd.DataFrame()
    #dft=dft.append(dft,ignore_index=True)
    for i in c:
        #print type(i)
        #print i["spidertime"]   #输出列表

        df=pd.Series(i["spidertime"])
        df= df[(df>st) & (df<et)]
        lst=df.max()
        sst=df.min()
        #print lst,sst
        #print sst
        sc= df.count()*5
        #算时长  #sc1=Series({"sc":sc})  
        df1=pd.Series(i).append(Series({"时长":sc}))
        df1["stime"]=lst
        df2=df2.append(df1,ignore_index=True)
        
    #print df1
    print df2
    try:
        dfa=df2.loc[:,['stime','ArticleTitle',u'融360','link','ArticlePubTime','ArticleFrom',u'媒体','时长','关键词','计分日','是否为理财文章']]
        #print dfa
        dfa['lx']=dfa['ArticleFrom'].str.contains(u'融360')
        dfa['hp']=dfa['ArticleTitle'].str.contains(u'融360')
        #dfa['']
        dfa1=dfa.loc[(dfa[u'融360']>0)|(dfa['hp']==True)|(dfa['lx']==True),['stime','hp','ArticleTitle',u'融360','link','ArticlePubTime','ArticleFrom','lx',u'媒体','时长']]
        print dfa1
        for r in dataframe_to_rows(dfa1,index=False,header=False):
            sf()
            if r[7] is True:
                r[7]=u"转载"
            else:
                r[7]=u"引用"
            if r[1] is True:
                r[1]=u"是"
            else:
                r[1]=u"否"
            
            r.append(fs)
            r.append("融360")
            r.append(x)


            ws1.append(r)
    except:
        ws1.append([])

#网贷之家


        
    

    #print r[2]
wb.save(u"/Users/jorylu/Desktop/合并"+str(x)+"_rong360.xlsx")

    #print i
    #print "123"


