## Ray programming constraints
This a list of the limitations and issues I have encountered in using Ray. It is expected that a distributed compute library will have many limitations/restrictions/rules that constrain the programming model, but these were not obvious after reading the Ray documentation.

    - Cannot use inheretence with ray.remote class. technicallly, you can sub calss a non ray.remote class, but I have not been able to call super on the base class, error like  super() argument 1 must be type, not ActorClass(AvgApply), making inheretence not very useful.
    - Cannot dynamically assign a function to a ray.remote class without using a proxy function.