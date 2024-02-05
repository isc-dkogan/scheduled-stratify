from pydantic import BaseModel
import pydantic

class Step:
    def __init__(self, function):
        self.function = function
     
    def __call__(self, *args, **kwargs):
        
        # First arg will always be the context or named and in kwargs
        ctx = kwargs.get("ctx") if kwargs.get("ctx") else args[0]
        print(f"ctx {ctx}, args - {args} kwargs - {kwargs}")
        
        # Model, if supplied, will be used to validate
        model = kwargs.get("model")
        if model:
            print(f'Validate {model}')
            config = model.parse_obj(ctx)
        
        self.function(*args, **kwargs)
 
class MyModel(BaseModel):
    name: str
    
@Step
def my_step(ctx, model: BaseModel = None):
     print("calling function")
     
if __name__ == "__main__":
    ctx = {"name": 1}
    my_step(ctx, model=MyModel)