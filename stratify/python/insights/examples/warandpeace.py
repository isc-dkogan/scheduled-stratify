from insights.util.core import DataProcessor
from insights.model.config import Context
from collections import defaultdict
import os
from logging import getLogger
import random 
import ray 

logger = getLogger(__name__)
    
@ray.remote
class ActorApply():
    def __init__(self, ctx):
        self.ctx = ctx
        
    def apply(self, item, idx):
        return {item[0]: item[1]}
    
if __name__ == '__main__':
    """An example of using DataProcesssor to execute a word counting function on a text file
    """
    
    ctx = Context(config = {"run_type": "test"}, job_id = -9)

    def split_func():
        
        with open("tests/data/warandpeace.txt") as fi:
            clob = ""
            for idx, line in enumerate(fi.readlines()):
                clob += line
                if idx % 10 == 0:
                    yield clob
                    clob = ""
                    
            # yield final lines
            yield clob

    @ray.remote
    def apply_func(item, ctx, idx):
        
        split_string = ((item.lower()).split())
        words = defaultdict(int)
        for word in split_string:
            words[word] += 1
            
        return words
    
    def accumulate_func(full_results, result, task_data):
        logger.debug(f"Full Results: {full_results}, result: {result})")
        
        for k, v in result.items():
            full_results[k] += v
            
        return full_results
    
    dp = DataProcessor("wordcount", ctx, split_func, apply_func, accumulate_func, None, 
                       parallelization=6, results_type=int)
    
    ctx = dp.run()
    print(ctx.wordcount)
    del dp
    
    # Single thread 
    # start - 2023-11-02 08:57:05.140 
    # end   - 2023-11-02 09:04:45.893 
    
    # Single thread
    # batch of 10
    # 2023-11-02 09:27:51.468 
    # 2023-11-02 09:28:37.161 
    
    # 6 threads
    # batch of 10
    # 2023-11-02 09:30:46.436 
    # 2023-11-02 09:31:18.756 