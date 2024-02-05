from insights.model.config import Context

def resolve(args: list, ctx: Context, kwargs: bool = False):
    """"
    Get the value from the context if it exists, otherwise get it from the globals and seet it in the context
    """
    new_args = [] if not kwargs else {}
    
    if args:
        for arg in args:
            if arg.startswith("@"):
                basename = arg.replace('@','')
            else: 
                basename = arg
            
            #from_ctx = ctx.get(basename)
            from_ctx = getattr(ctx, basename) if hasattr(ctx, basename) else None            
            if from_ctx:
                value = from_ctx
            elif globals()[basename] is not None:
                value = globals()[basename]
                setattr(ctx, basename, value)
            else:
                raise Exception(f"arg {arg} not found in context or globals")

            if not kwargs:
                new_args.append(value)
            else:
                new_args[basename] = value
    
    print(f"Args and new args {args}, {new_args}")
    return new_args

def call_function(funk, args = None, kwargs = None):
    
    if args and kwargs:
        new_val = funk(*args, **kwargs)
    elif args and not kwargs:
        new_val = funk(*args)
    elif not args and kwargs:
        new_val = funk(**kwargs)
    elif not args and not kwargs:
        new_val = funk()
        
    return new_val

def register_namespace(funklist: list):

    for f in funklist:
        print(f"Adding {f} as {f.__name__}")
        globals()[f.__name__] = f


def process_workflow(ctx: Context, workflow: dict):
    
    print(f"Cont {ctx}")
    stages = workflow.get("stages")
    for stage in stages:
        print(f"Stage {stage.get('name')}")
        steps = stage.get("steps")
        for step in steps:
            
            # Set local vars for step
            step_name = step.get('name')
            function_obj = step.get('function')
            function_name = function_obj.get("name")
            print(f"Step {step_name}, function {function_name}")
        
            # Set steps function, it's args, kwargs and return variable
            funk = globals()[function_name]
            args = resolve(function_obj.get("args"), ctx)
            kwargs = resolve(function_obj.get("kwargs"), ctx, kwargs=True)
            
            # Exception handling for function
            # Todo - make this more robust or likely complete the compile step which does this before running
            # The compile and run code might be the same code where compile is a dry run
            if not funk:
                raise Exception(f"{function_name} cannot be found in global namespace")
            if not callable(funk):
                raise Exception(f"{function_name} is not callable")
            
            # Call the function and set the return value in the context
            print(f"Calling {funk.__name__} with {args} and {kwargs}")
            new_val = call_function(funk, args, kwargs)
            print(f"Returned {new_val}")
            setattr(ctx, function_obj.get("output"), new_val)
            
    return ctx

