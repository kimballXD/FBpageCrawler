# -*- coding: utf-8 -*-
"""
Created on Fri Mar 10 14:58:04 2017

@author: Wu
"""
import os
ROOT_PATH='c:/coding/FBapi'
os.chdir(ROOT_PATH)
import pagecrawler
import numpy as np
import pandas as pd
import pickle

#%%

#crawler setting
initium='466505160192708'
reporter='1646675415580324'
pts='359437115654'
cw='127628276929'
#crawlerInfoList=[(initium,u'端傳媒'),(reporter,u'報導者'),(pts,u'公視'),(cw,u'天下')]
crawlerInfoList=[(cw,u'天下')]
fields='id,shares,message,link,status_type,type,created_time,permalink_url'
userToken=

#crawl!
crawlerList=[]
for pageId, pageName in crawlerInfoList:
    print(u'start crawling {}'.format(pageName))
    crawler=pagecrawler.PageCrawler(pageId, pageName, userToken)
    crawler.get_pageInfo(fields='fan_count')
    crawler.get_posts(startDate='2017-03-01',endDate='2017-03-02', sleep=1, limit=100, fields=fields)
    crawler.get_post_connections(connection_name='reactions',limit=5000)
    crawler.get_post_connections(connection_name='comments',limit=5000)
    crawler.get_post_connections(connection_name='sharedposts',limit=5000)
    crawler.get_unique_user_id()
    crawlerList.append(crawler)
    print(u'{} finish crawling'.format(pageName))

#save
duration=crawlerList[0].batchInfo['endFilter'].isoformat()+'_'+crawlerList[0].batchInfo['startFilter'].isoformat()
pickleName='crawlerList_{}.pickle'.format(duration)
with open(pickleName,'wb') as outFile:
    pickle.dump(crawlerList,outFile)

#%%

#simple crawl info

infoList=[]
for x in crawlerList:
    temp=[x.pageName, x.batchInfo['endFilter'], x.batchInfo['startFilter'], x.batchInfo['startTime'], x.pageInfo['fan_count'], len(x.postList)]
    temp.extend([len(y) for y in  x.POST_CONNECTION_STORAGE.values()])
    temp.extend([len(y) for y in  x.POST_CONNECTION_FAILED_STORAGE.values()])
    infoList.append(temp)
columns=['pageName','startDate','endDate','crawling_startTime','fan_count','posts','reactions','comments','sharedposts','crawlFailed_reaction','crawlFailed_comment','crawlFailed_sharedposts']
crawlerInfo=pd.DataFrame(infoList,columns=columns)
print('\n\nInfo of every pageCrawler')
print(crawlerInfo)


#simple compute
cralwerNameList=[x.pageName for x in crawlerList]
edgelen=len(crawlerList)
commonMat=np.ones(edgelen**2).reshape(edgelen,edgelen)
for i,objI  in enumerate(crawlerList):
    for j,objJ in enumerate(crawlerList):
        if i!=j:
            commonMat[i,j]=len(objI.uniqueUser.intersection(objJ.uniqueUser))/float(len(objI.uniqueUser))
        else:
            continue
commonDF=pd.DataFrame(commonMat, index=cralwerNameList, columns=cralwerNameList)
commonDF=commonDF.applymap(lambda x:x*100)
print('\n\n\nuser overlapping percetage between media fan pages in the posts in the duration {}'.format(duration))
print(commonDF)
