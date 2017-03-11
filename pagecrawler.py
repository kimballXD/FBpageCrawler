# -*- coding: utf-8 -*-
"""
Created on Thu Mar 02 00:36:24 2017

@author: Wu
"""
import time
import random
import datetime
import sys
import multiprocessing.dummy as mt
import facebook
import requests
import pageCrawlerUtil

#%%

class PageCralwer(facebook.GraphAPI):   
    def __init__(self,pageID, pageName, access_token, version=None, timeout=None, proxies=None):
        super(PageCralwer,self).__init__(version, timeout, proxies)
        self.access_token=access_token
        self.pageID=pageID
        self.pageName=pageName
        self.batchInfo=None
        self.postList=[]
        self.postLike=[]
        self.postShare=[]
        self.postComment=[]
        self.likeFailed=[]
        self.shareFailed=[]
        self.commentFailed=[]
        self.uniqueUser=set()
        self.POST_CONNECTION_STORAGE={'reactions':self.postLike,'sharedposts':self.postShare,'comments':self.postComment}
        self.POST_CONNECTION_FAILED_STORAGE={'reactions':self.likeFailed,'sharedposts':self.shareFailed,'comments':self.commentFailed}
    
    def _popArgs(self,kwargs):
            [kwargs.pop(x,None) for x in ['fields','limit']]
            return kwargs
    
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
    
    def _combine_res(self, res, obj, batchInfo):
        temp=obj['data']
    
        #no Filter
        if not batchInfo:
            res['started']=True
            res['resList'].append(temp)
            return res           
            
        #check for the start
        elif (batchInfo['filterType']=='both' or batchInfo['filterType']=='until') and not res['started']:
            condValue=batchInfo['startFilter']
            postDate=[datetime.datetime.strptime(x['created_time'].split('T')[0],'%Y-%m-%d').date() for x in temp]
            minDate=min(postDate)
            if minDate < condValue:
                res['started']=True
                print('just appear one time!(start)')
                reserveFilter=[x <= condValue for x in postDate]
                reserveItem=[temp[ind] for ind, x in enumerate(reserveFilter) if x]
                obj['data']=reserveItem
                return self._combine_res(res, obj , batchInfo)
            else:
                return res      

        # check for the ends
        elif batchInfo['filterType']=='since' or (batchInfo['filterType']=='both' and res['started']):
            res['started']=True
            condValue=batchInfo['endFilter']
            postDate=[datetime.datetime.strptime(x['created_time'].split('T')[0],'%Y-%m-%d').date() for x in temp]
            minDate=min(postDate)
            if minDate<condValue:
                print('just appear one time!(end)')
                res['ended']=True
                reserveFilter=[x>=condValue for x in postDate]
                reserveItem=[temp[ind] for ind, x in enumerate(reserveFilter) if x]
                res['resList'].extend(reserveItem)
            else:
                res['resList'].extend(temp)
            return res

    def _crawl_paging_obj(self, nextPage, res, batchInfo, sleep, **kwargs):
        kwargs=self._popArgs(kwargs)
        while nextPage:
            page=requests.get(nextPage, **kwargs).json()
            print('sleep for {} seconds'.format(sleep))
            time.sleep(sleep)
    
            res=self._combine_res(res, page, batchInfo)
            nextPage=page['paging']['next'] if 'paging' in page and 'next' in page['paging'] else None                               
            if res['ended'] or not nextPage:
                return res
            
    def get_posts(self, startDate=None, endDate=None, sleep=1, **kwargs):
        #check and preparing input
        batchInfo={}
        temp={0:'endFilter',1:'startFilter'} # notice that meaning of filter were reverse with meaning of start/end Date
        for idx, x in enumerate([startDate, endDate]):
            if x:         
                try:
                    date=datetime.datetime.strptime(x,'%Y-%m-%d').date()
                except:
                    print('[FAILED] startDate/endDate should be a date string following "YYYY-mm-dd" formate:{}'.format(x))
                    sys.exit()
            batchInfo.update({temp[idx]:date})
        if len(batchInfo)==1:
            batchInfo.update({'filterType':'since'}) if startDate else batchInfo.update({'filterType':'until'})
        elif len(batchInfo)==2:
            batchInfo.update({'filterType':'both'})
        
        res={'resList':[],'started':False,'ended':False,'endCond':None}
    
        #process
        startTime=datetime.datetime.now()
        entryObj=self.get_connections(self.pageID, connection_name='posts', **kwargs)              
        res=self._combine_res(res, entryObj, batchInfo)
        nextPage=entryObj['paging']['next'] if 'paging' in entryObj and 'next' in entryObj['paging'] else None                        
        if not res['ended'] and nextPage:
            self.postList=self._crawl_paging_obj(nextPage, res, batchInfo, sleep, **kwargs)['resList']
        
        # ending
        res['endCond']='ended' if res['ended'] else 'noNextPage'
        self.postList=res['resList']
        for x in ['started','ended','endCond']:
            batchInfo.update({x:res[x]})
        batchInfo.update({'startTime':startTime.strftime('%Y-%m-%d %H:%M:%S')})
        self.batchInfo=batchInfo
                

    def get_post_connections(self,connection_name,**kwargs):    
        if not self.postList:
            print('there is no post for crawling {}, call get_posts() first'.format(connection_name))
            sys.exit()

        manager=mt.Manager()
        producerPool=mt.Pool(4)
        consumerPool=mt.Pool(2)
        produceQueue=manager.Queue()
        resQueue=manager.JoinableQueue()
                
        for part in pageCrawlerUtil.partitioner(self.postList,20):
            partIDs=[x['id'] for x in part]
            produceToken=producerPool.apply_async(func=self.get_objs_connections,
                                                  args=(partIDs, connection_name,),
                                                  kwds=kwargs)
            produceQueue.put(produceToken)
            print('sleep {} second for every producer commit'.format(1))
            time.sleep(1)
#                        
        #make sure all get_objs_connection works are done
        while produceQueue.qsize()>0:
            print('Tasks in the produceQueue:{}.'.format(produceQueue.qsize()))
            produceRes=produceQueue.get()
            
            if produceRes.ready() and produceRes.successful():
                for x in produceRes.get().items():
                    consumeToken=consumerPool.apply_async(func=self._pool_crawl_paging_obj,
                                                          args=(x,consumerPool,'obj'),
                                                          kwds=kwargs)
                    resQueue.put(consumeToken)
                if len(produceRes.get())>1:
                    print('sleep {} second for every consumer crawl_paging batch commit'.format(2))
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
                time.sleep(random.random())
                print('wait another random second to wait for the produce done!')        
        print('all produce works OK!')
                
        #make sure all paging are crawled back
        tempList=[]
        while resQueue.qsize()>0:
            print('Task in the consume queue: {}.'.format(resQueue.qsize()))
            consumeRes=resQueue.get()
            if consumeRes.ready():
                tempList.append(consumeRes.get())
                resQueue.task_done()
            else:
                resQueue.put(consumeRes) #put bac
                time.sleep(random.random())             
                print('wait another random second to wait for the consume done!')
        resQueue.join()
        print('all paging consume work ok!')
        
        #get all share user ID for sharedpost
        if connection_name=='sharedposts':
            shareUserList=[x['id'] for objId, data in tempList for x in data if x]
            kwargs.update({'fields':'from,parent_id'})
            tempShareUser=[]
            for partId in pageCrawlerUtil.partitioner(shareUserList,20):
                consumeToken=consumerPool.apply_async(func=self.get_objects,
                                                      args=(partId,),
                                                      kwds=kwargs)
                tempShareUser.append(consumeToken)
                print('sleep for random second for every batch submit')
                time.sleep(random.random())
            consumerPool.close()
            consumerPool.join()
            sharedUser=dict()
            [sharedUser.update(x.get()) for x in tempShareUser]
            
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

    def _pool_crawl_paging_obj(self, objTuple, pool, objType, **kwargs):
        kwargs=self._popArgs(kwargs)
        if objType=='url':
            objId, url=objTuple
            print('done 1 request for conneciton paging, though dont sleep')
            obj=requests.get(url, **kwargs).json()
            objTuple=(objId,obj)
            
        objId, obj=objTuple
        res=(objId, obj['data'])
        if 'paging' in obj and 'next' in obj['paging']:
            url=obj['paging']['next']
            nextRes=pool.apply_async(self._pool_crawl_paging_obj,
                                     args=((objId,url), pool, 'url',),
                                     kwds=kwargs)
        return res
          
    def get_unique_user_id(self):
        res=[]
        res.extend([x['id'] for x in self.postLike])
        res.extend([x['from']['id'] for x in self.postComment])
        res.extend([x['userID'] for x in self.postShare])
        self.uniqueUser=set(res)
        return 



#%%
