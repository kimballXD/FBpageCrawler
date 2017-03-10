# -*- coding: utf-8 -*-
"""
Created on Fri Mar 10 14:21:33 2017

@author: Wu
"""


def partitioner(data,num):
    """
    input:
        data: list-like object, can be sliced by index
        num: the number of item per partition
    output:
        partitioned data iterator
    """
    import math
    partitions=int(math.ceil(float(len(data))/num))
    for i in range(partitions):
         yield data[num*i:num*(1+i)]   
         

class Rester():
    def __init__(self,interval):
        self.interval=interval
        self.count=0
    def interval_sleep(self,sleep,msg=None):
        self.count=self.count+1
        if self.count==self.interval:
            self.count=0
            time.sleep(sleep)
            msg='Sleep {} second for every {} rounds'.format(sleep, self.interval) if not msg else msg
            return msg
        