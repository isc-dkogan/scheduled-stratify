from insights.job.execute_ctx import execute_tasks
from insights.model.config import Context
import ray 
from time import sleep

def test_execute_ctx():
    
    # A Context object is required. THis is one with just required fields
    ctx = Context(config={"run_type": "test"}, job_id=-99)
    
    # The iterator generates each task
    iterator = [i for i in range(10)]
    
    # The function executes the task and returns the result
    @ray.remote
    def funk(item, ctx, idx):
        return idx   
    
    # The accumulator collects all the results by an id
    def accum(item, full_results, result, idx):
        full_results[idx] += result 
        return full_results
    
    ctx = execute_tasks(iterator, funk, 4, accum, ctx, "Test", int)
    
    print(ctx)

if __name__ == "__main__":
    test_execute_ctx()
    ray.shutdown()