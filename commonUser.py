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
import cPickle

#%%

#crawler setting
initium='466505160192708'
reporter='1646675415580324'
pts='359437115654'
cw='127628276929'
crawlerInfoList=[(initium,u'端傳媒'),(reporter,u'報導者'),(pts,u'公視'),(cw,u'天下')]
fields='id,shares,message,link,status_type,type,created_time,permalink_url'
userToken='EAARyVGSlbYYBAE9kBPxLKN3FY1nrS0SR379RvAdVjxssFU7ZAPjAr4qLwDboP5UNVF2MbUpGGqmU0jDjezV7R2lC4xDzHzFZBQ9AG2oU3TPCM13RV048MHffHU07ZAPniZBxt7gZAzeT93V4ZCZC8TW1DUTejgcl9cZD'

#crawl!
crawlerList=[]
for pageId, pageName in crawlerInfoList:
    print u'start crawling {}'.format(pageName)
    crawler=pagecrawler.PageCralwer(pageId, pageName, userToken)
    crawler.get_posts(startDate='2017-03-01',endDate='2017-03-02', sleep=1, limit=100, fields=fields)
    crawler.get_post_connections(connection_name='reactions',limit=5000)
    crawler.get_post_connections(connection_name='comments',limit=5000)
    crawler.get_post_connections(connection_name='sharedposts',limit=5000)
    crawler.get_unique_user_id()
    crawlerList.append(crawler)
    print u'{} finish crawling'.format(pageName)

with open('crawlerList.pickle','wb') as outFile:
    cPickle.dump(crawlerList,outFile)

#%%

#simple crawl info


#simple compute
cralwerNameList=[x.pageName for x in crawlerList]
edgelen=len(crawlerList)
commonMat=np.zeros(edgelen**2).reshape(edgelen,edgelen)
for i,objI  in enumerate(crawlerList):
    for j,objJ in enumerate(crawlerList):
        if i!=j:
            commonMat[i,j]=len(objI.uniqueUser.intersection(objJ.uniqueUser))/float(len(objI.uniqueUser))
        else:
            continue
commonDF=pd.DataFrame(commonMat, index=cralwerNameList, columns=cralwerNameList)
commonDF=commonDF.applymap(lambda x:x*100)
print commonDF
