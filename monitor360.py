#coding=utf-8
import re
from pymongo import MongoClient
mc=MongoClient("localhost",27017)
import pandas as pd
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import Workbook
import sys
import xlrd
reload(sys)
sys.setdefaultencoding('utf-8')
db=mc.monitor360
wb = Workbook()
ws = wb.active
ws.append(['爬取时间', '标题是否含有关键词', '文章标题', '监控词出现次数', '文章链接','文章推荐发布时间', '来源', '类型','推荐位置', '推荐时长(分钟)','得分','关键词','计分日'])
def monitor(key_word,bd,ed):
    
    for day in range(int(bd),int(ed),1):
        print day

        print key_word#=关键词
            #bd=开始日
            #ed=结束日
        reExp=re.compile(r'.*'+key_word+r'.*')
        print reExp.pattern
        #c =db.m51.find({u"正文":reExp})
        try:
            c=db.test2.find(
                {"$and":[
                    {"spidertime":{"$elemMatch":{"$gte":"0"+str(day)+"-0000","$lte":"0"+str(day)+"-2400"}}},
                    {"$or":[{u"正文":reExp},
                            {u"ArticleTitle":reExp},
                            {u"ArticleFrom":reExp}
                           ]}

                ]}
            )
            bs_all=pd.DataFrame(list(c))
            #print bs_all['spidertime']#[(bs_all['spidertime'])>"0927-0000" & (bs_all['spidertime'])<"0928-0000"]
            #print bs_all['spidertime']#.max
            '''
            for x in bs_all['spidertime']:
                x=pd.Series(x)
                x=x[(x>"0927-0000") & (x<"0927-1200")]
                #print x.size*5.0/60
            '''


            def jishu(x):
                x=pd.Series(x)
                x=x[(x>"0"+str(day)+"-0000") & (x<"0"+str(day)+"-2400")]
                #print type(x)
                return x.size*5.0
            #用函数的写法是这样
            bs_all['online_time']=map(lambda t:jishu(t),bs_all['spidertime'])
            #直接用lambda不大可以这么写

            def paTime_last(x):
                x=pd.Series(x)
                x=x[(x>"0"+str(day)+"-0000") & (x<"0"+str(day)+"-2400")]
                return "".join(x.tail(1).values)
            bs_all['day_lasttime']=map(lambda x:paTime_last(x),bs_all['spidertime'])


            bs_all['keyword_count']=map(lambda x:"".join(x).count(key_word),bs_all[u'正文'])


            bs_all['article_type']=map(lambda x: "转载" if key_word in x else "引用",bs_all['ArticleFrom'])
            bs_all['title_key']=map(lambda x: "包含" if key_word in x else "不包含",bs_all['ArticleTitle'])
            bs_all['key_word']=key_word
            bs_all['day']=day
            #print type(bs_all['ArticleFrom'])
            b=bs_all[bs_all['online_time']>0].drop(["_id","haspoint","spidertime",u'的',u'正文',u'盈灿咨询',u'网贷之家',u'网贷天眼',u'融360',u'零壹财经'],axis=1) #["shichang"]=map
            def sf(a,b,c,d):
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
                stander=medi.get(a.encode("utf-8"))

                
                #print type(stander)

                
                if b =="包含":
                    k=5.0
                else:
                    k=0.0
                    
                #【c】出现次数计分
                if c==0:
                    p=0.0
                elif c==1:
                    p=1
                elif c==2:
                    p=1.5
                elif c==3:
                    p=1.8
                else:
                    p=2.0
                

                ##时长记分算之前的一半
                if d<=480:
                    ts=d*0.5/120
                elif d<=720:
                    ts=2 + (d - 480) * 0.375 / 120
                elif d<960:
                    ts=2.75 + (d - 720) * 0.25 / 120
                else:
                    ts=3.25
             
                return round(stander*(k+p+ts),2)
                
            b['score']=map(lambda a,b,c,d:sf(a,b,c,d),b[u'媒体'],b['title_key'],b['keyword_count'],b['online_time'])
            b.rename(columns={u'媒体':u"media"},inplace=True)
            cc=list([u'day_lasttime',u'title_key',u'ArticleTitle',u'keyword_count',u'link',u'ArticlePubTime',u'ArticleFrom',u'article_type',u'media',u'online_time',u'score',u'key_word',u'day'])
            bb=b.reindex(columns=cc)
            print bb.columns

            for r in dataframe_to_rows(bb, index=False, header=False):
                ws.append(r)
            print bb
            print bb['score'].values#loc[:,u'link'].values.tolist()#.encode("utf8")
        except:
            next
        #ws.append([])
    #ws.append([])
#monitor(u'苏宁金融研究院','0928-0000','0929-1200')
#[u'融360',u'网贷之家',u'盈灿资讯',u'苏宁金融研究院']
#[u'银行存管',u'货币基金',u'网贷监管',u'银行理财收益',u'直销银行',u'房贷利率',u'现金贷',u'信用卡',u'网贷评级']
[monitor(ke,'0901','0931') for ke in ['网贷之家','盈灿咨询','苏宁金融研究院']]

wb.save(u"/Users/jorylu/Desktop/媒体影响力——9月(新打分规则).xlsx")

'''                ##【ts】时长计分
                if d<=480:
                    ts=d*0.5/60
                elif d<=720:
                    ts=4 + (d - 480) * 0.375 / 60
                elif d<960:
                    ts=5.5 + (d - 720) * 0.25 / 60
                else:
                    ts=6.5
'''