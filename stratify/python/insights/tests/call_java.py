import ray

with ray.init(job_config=ray.job_config.JobConfig(code_search_path=["/Users/psulin/projects/insights/java/insights/target"])):
    # Define a Java class.
    counter_class = ray.cross_language.java_actor_class(
            "com.iscfs.App")

    # # Create a Java actor and call actor method.
    # counter = counter_class.remote()
    # obj_ref1 = counter.increment.remote()
    # assert ray.get(obj_ref1) == 1
    # obj_ref2 = counter.increment.remote()
    # assert ray.get(obj_ref2) == 2

    # Define a Java function.
    add_function = ray.cross_language.java_function(
            "com.iscfs.App", "add")

    # Call the Java remote function.
    obj_ref3 = add_function.remote(1, 2)
    result =  ray.get(obj_ref3)
    print(f"Result {result}")
    assert result == 3