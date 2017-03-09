# -*- coding: utf-8 -*-
"""
Created on Thu Mar 02 00:36:24 2017

@author: Wu
"""
import sqlite3
import pandas as pd
import facebook
import requests
import time
import datetime
import sys
import multiprocessing.dummy as mt
import random
import crawlerUtil
initium='466505160192708'
reporter='1646675415580324'
userToken='EAARyVGSlbYYBAE9kBPxLKN3FY1nrS0SR379RvAdVjxssFU7ZAPjAr4qLwDboP5UNVF2MbUpGGqmU0jDjezV7R2lC4xDzHzFZBQ9AG2oU3TPCM13RV048MHffHU07ZAPniZBxt7gZAzeT93V4ZCZC8TW1DUTejgcl9cZD'

#%%

class PageCralwer(facebook.GraphAPI):   
    def __init__(self,pageID,access_token,version=None, timeout=None, proxies=None):
        super(PageCralwer,self).__init__(version, timeout, proxies)
        self.access_token=access_token
        self.pageID=pageID
        self.pageName=None
        self.postFilter=None
        self.crawlPostTime=None
        self.postList=[]
        self.postLike=[]
        self.postShare=[]
        self.postComment=[]
        self.likeFailed=[]
        self.shareFailed=[]
        self.commentFailed=[]
        self.uniqueUser=[]
        self.POST_CONNECTION_STORAGE={'reactions':self.postLike,'sharedposts':self.postShare,'comments':self.postComment}
        self.POST_CONNECTION_FAILED_STORAGE={'reactions':self.likeFailed,'sharedposts':self.shareFailed,'comments':self.commentFailed}

                
    def get_objs_connections(self,ids,connection_name,**args):
        args['ids']=','.join(ids)
        try:
            return self.request("%s/%s" % (self.version, connection_name), args)
        except Exception as e:
            e.ids=ids
            raise e
            
    def get_objs_connections_single_excpetion(self, postId, connection_name, **args):
        try:
            return {postId:self.request("%s/%s/%s" % (self.version, postId, connection_name), args)}
        except Exception as e:
            e.id=postId
            raise e
    
    def _combine_res(self, res, obj, postFilter):
        temp=obj['data']
    
        #no Filter
        if not postFilter:
            res['started']=True
            res['resList'].append(temp)
            return res           
            
        #check for the start
        elif postFilter['filterType']=='both' and not res['started']:
            condValue, condType =postFilter['startFilter']
            if condType=='int':
                if len(temp)+res['skip']>condValue:
                    res['started']=True
                    obj['data']=temp[condValue-skip:]
                    return self._combine_res(res, obj ,postFilter)
                else:
                    res['skip']=res['skip']+len(temp)
                    return res
            elif condType=='date':
                postDate=[datetime.datetime.strptime(x['created_time'].split('T')[0],'%Y-%m-%d').date() for x in temp]
                minDate=min(postDate)
                if minDate < condValue:
                    res['started']=True
                    print 'just appear one time!(start)'
                    reserveFilter=[x <= condValue for x in postDate]
                    reserveItem=[temp[ind] for ind, x in enumerate(reserveFilter) if x]
                    obj['data']=reserveItem
                    return self._combine_res(res, obj , postFilter)
                else:
                    return res      
    
        # check for the ends
        elif postFilter['filterType']=='until' or (postFilter['filterType']=='both' and res['started']):
            res['started']=True
            condValue, condType =postFilter['endFilter']
            
            if condType=='int':
                res['resList'].extend(temp)
                if len(res['resList'])+len(temp)>=condValue:
                    res['resList']=res['resList'][:condValue]
                    res['ended']=True
                return res
            elif condType=='date':
                postDate=[datetime.datetime.strptime(x['created_time'].split('T')[0],'%Y-%m-%d').date() for x in temp]
                minDate=min(postDate)
                if minDate<condValue:
                    print 'just appear one time!(end)'
                    res['ended']=True
                    reserveFilter=[x>=condValue for x in postDate]
                    reserveItem=[temp[ind] for ind, x in enumerate(reserveFilter) if x]
                    res['resList'].extend(reserveItem)
                else:
                    res['resList'].extend(temp)
                return res

    def _crawl_paging_obj(self, nextPage, res, sleep=1, postFilter=None):        
        while nextPage:
            page=requests.get(nextPage).json()
            print 'sleep for {} seconds'.format(sleep)
            time.sleep(sleep)
    
            res=self._combine_res(res, page, postFilter)
            nextPage=page['paging']['next'] if page.has_key('paging') and page['paging'].has_key('next') else None                               
            if res['ended'] or not nextPage:
                return res
            
    def get_posts(self, postFilter=None, sleep=1, **args):
        #check and preparing input
        if postFilter:
            filterTemp=[]
            for idx, x in enumerate(postFilter):
                condType=type(x).__name__                       
            if condType=='str':
                try:
                    x=datetime.datetime.strptime(x,'%Y-%m-%d').date()
                    condType='date'
                except:
                    print 'If input postfilter as string, should be a date string following iso date format:{}'.format(x)
                    sys.exit()
                filterTemp.append((x,condType))
            if len(filterTemp)==1:
                postFilter={'filterType':'until','endFilter':filterTemp[0]}
            elif len(filterTemp)==2:
                postFilter={'filterType':'both','startFilter':filterTemp[1],'endFilter':filterTemp[0]}
        
        res={'resList':[],'started':False,'ended':False,'skip':0,'endCond':None}
        
        #process
        entryObj=self.get_connections(self.pageID, connection_name='posts', **args)              
        res=self._combine_res(res, entryObj, postFilter)
        nextPage=entryObj['paging']['next'] if entryObj.has_key('paging') and entryObj['paging'].has_key('next') else None
                         
        if not res['ended'] and nextPage:
            self.postList=self._crawl_paging_obj(nextPage, res, postFilter=postFilter)['resList']
        
        # ending
        res['endCond']='ended' if res['ended'] else 'noNextPage'
        self.postList=res['resList']
        self.postFilter=postFilter
        self.crawlPostTime=datetime.datetime.now()
        
        
    def _pool_crawl_paging_obj(self, objTuple, pool, objType, **args):
        if objType=='url':
            objId, url=objTuple
            print 'done 1 request for conneciton paging, though dont sleep'
#            time.sleep(1)
            obj=requests.get(url, **kwargs ).json()
            objTuple=(objId,obj)
            
        objId, obj=objTuple
        res=(objId, obj['data'])
        if obj.has_key('paging') and obj['paging'].has_key('next'):
            url=obj['paging']['next']
            nextRes=pool.apply_async(self._pool_crawl_paging_obj,
                                     args=((objId,url), pool, 'url',),
                                     kwds=args)
        return res
            
    def get_post_connections(self,connection_name,**kwargs):
      
        if not self.postList:
            print 'there is no post for crawling {}, call get_posts() first'.format(connection_name)
            sys.exit()

        manager=mt.Manager()
        producerPool=mt.Pool(4)
        consumerPool=mt.Pool(2)
        produceQueue=manager.Queue()
        resQueue=manager.JoinableQueue()
                
        for part in crawlerUtil.partitioner(self.postList,20):
            partIDs=[x['id'] for x in part]
            produceToken=producerPool.apply_async(func=self.get_objs_connections,
                                                  args=(partIDs, connection_name,),
                                                  kwds=kwargs)
            produceQueue.put(produceToken)
            print 'sleep {} second for every producer commit'.format(1)
            time.sleep(1)
#                        
        #make sure all get_objs_connection works are done
        while produceQueue.qsize()>0:
            print 'Tasks in the produceQueue:{}.'.format(produceQueue.qsize())
            produceRes=produceQueue.get()
            
            if produceRes.ready() and produceRes.successful():
                for x in produceRes.get().iteritems():
                    consumeToken=consumerPool.apply_async(func=self._pool_crawl_paging_obj,
                                                          args=(x,consumerPool,'obj'),
                                                          kwds=kwargs)
                    resQueue.put(consumeToken)
                if len(produceRes.get())>1:
                    print 'sleep {} second for every consumer crawl_paging batch commit'.format(2)
                    time.sleep(2)
            elif produceRes.ready() and not produceRes.successful():
                try:
                    produceRes.get()
                except Exception as e:
                    if hasattr(e,'id'):
                        self.POST_CONNECTION_FAILED_STORAGE[connection_name].append((e.id, e.message))
                    elif hasattr(e,'ids'):
                        for x in e.ids:
                            produceToken=producerPool.apply_async(func=self.get_objs_connections_single_excpetion,
                                                                  args=(x, connection_name),
                                                                  kwds=kwargs)
                            produceQueue.put(produceToken)
            else:
                produceQueue.put(produceRes) #put back
                time.sleep(2)
                print 'wait another 2 second to wait for the produce done!'
        
        print 'all produce works OK!'
                
        #make sure all paging are crawled back
        tempList=[]
        while resQueue.qsize()>0:
            print 'Task in the consume queue: {}.'.format(resQueue.qsize())
            consumeRes=resQueue.get()
            if consumeRes.ready():
                tempList.append(consumeRes.get())
            else:
                resQueue.put(consumeRes) #put bac
                time.sleep(random.random())             
                print 'wait another random second to wait for the consume done!'
        
        print 'all paing consume work ok!'
        
        #get all share user ID for sharedpost
        if connection_name=='sharedposts':
            tempShareUser=[]
            for part in crawlerUtil.partitioner(tempList,20):
                partId=[x['id'] for objId, data in part for x in data if x]
                kwargs.update({'fields':'from,parent_id'})
                consumeToken=consumerPool.apply_async(func=self.get_objects,
                                                      args=(partId,),
                                                      kwds=kwargs)
                tempShareUser.append(consumeToken)        
            consumerPool.close()
            consumerPool.join()
            sharedUser=dict()
            [sharedUser.update(x.get()) for x in tempShareUser]
#            sharedUser=dict([(v.get(['parent_id']),v['from']) for k,v in sharedUser.items() if v])
        #collect result
        resultList=[]
        for objId, data in tempList:
            if data:
                [item.update({'postID':objId}) for item in data]
                if connection_name=='sharedposts':
                    [item.update({'userID':sharedUser[item['id']]['from']['id']}) for item in data]
                    [item.update({'userName':sharedUser[item['id']]['from']['name']}) for item in data]
                resultList.extend(data)
        
        #return
        self.POST_CONNECTION_STORAGE[connection_name].extend(resultList)
        producerPool.close()
        
        return 
                
    def get_unique_user_id(self):
        res=[]
        res.extend([x['id'] for x in self.postLike])
        res.extend([x['from']['id'] for x in self.postComment])
        res.extend([x['userID'] for x in self.postShare])
        return set(res)

    
#%%
        
#    
initChan=PageCralwer(initium,userToken)
fields='id,shares,message,link,status_type,type,created_time,permalink_url',
initChan.get_posts(postFilter=('2017-03-01','2017-03-07'), limit=100, fields=fields)
initChan.get_post_connections(connection_name='reactions',limit=5000)
initChan.get_post_connections(connection_name='comments',limit=5000)
initChan.get_post_connections(connection_name='sharedposts',limit=5000)
uniqueUser=initChan.get_unique_user_id()
#%%

reportChan=PageCralwer(reporter,userToken)
fields='id,shares,message,link,status_type,type,created_time,permalink_url',
reportChan.get_posts(postFilter=('2017-02-01',), limit=100, fields=fields)
reportChan.get_post_connections(connection_name='reactions',limit=5000)
reportChan.get_post_connections(connection_name='comments',limit=5000)
reportChan.get_post_connections(connection_name='sharedposts',limit=5000)




