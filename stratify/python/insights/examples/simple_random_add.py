from insights.util.core import DataProcessor
from insights.model.config import Context
from collections import defaultdict

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
    """An example of using DataProcesssor to execute a counting function for randomly generated integers from 0 100
    It will run x iterations, based on iteration veraible and use y distinct keys based on key_count.
    By using a seed with random we can achieve deterministic results.
    """
    
    ctx = Context(config = {"run_type": "test"}, job_id = -9)
    
    random.seed(123)
    iterations = 20
    key_count = 4
    def split_func():
        for i in range(1,iterations):
            yield (random.randint(1,key_count), random.randint(0, 100),)
    
    @ray.remote
    def apply_func(item, ctx, idx):
        return {item[0]: item[1]}
    
    def accumulate_func(full_results, result, task_data):
        logger.debug(f"Full Results: {full_results}, result: {result})")
        
        for k, v in result.items():
            full_results[k] += v
            
        return full_results
    
    dp = DataProcessor("random", ctx, split_func, apply_func, accumulate_func, None, 
                       parallelization=2, results_type=int)
    
    ctx = dp.run()
    print(ctx.random)
    del dp
    
    # seed = 123
    # Task
    # {1: 556, 3: 435, 4: 559, 2: 304}
    # {1: 556, 3: 435, 4: 559, 2: 304}
    # 20 iters
    # {1: 272, 3: 111, 4: 382, 2: 167}
    # {1: 272, 3: 111, 4: 382, 2: 167}
    # {3: 111, 1: 336, 4: 382, 2: 167}
    # Actor
    # 40 iters
    # {1: 620, 3: 435, 4: 559, 2: 304})
    # {3: 435, 1: 620, 4: 559, 2: 304})
    # 20 iters
    # {1: 336, 3: 111, 4: 382, 2: 167})
    