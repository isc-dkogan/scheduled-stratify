"""
These are different ways to represent the same data structure that
is inherently tree-like or parent-child.

The hierarchical represents the data in the parent-child structure. This requires
accessing the data from a top down approach, always requiring to start with the root,
the access semantics matching the structure. The structure maintains the relationships.

The flat approach is a simpler structure that requires some more helper functions
to access objects. Each child contains a name reference to it's parent and 
contains a list of names of it's children. Access will be slower, requiring iterating a 
list instead of dict lookup, though I don't image for metadata this would be an issue.
But for larger structures it could be noticeable.
"""


flat_name_keyed = {
    "instances": [{"name": "b360", "databases": ["b360"]}],
    "databases": [{"name": "b360", "instance_name": "b360", "schemas": ["b360_model"]}],
    "schemas": [{"name": "b360_model", "database_name": "b360", "tables": ["table1"]}],
    "tables": [{"name": "table1", "schema_name": "b360_model", "columns": ["col1"]}],
    "columns": [{"name": "col1", "table_name": "table1", "datatype": "varchar"}],
}


heirarchical = {
    "instances": [
        {
            "name": "b360",
            "databases": [
                {
                    "name": "b360",
                    "schemas": [
       {
                            "name": "b360_Models",
                            "tables": [
                                {
                                    "name": "table1",
                                    "field_count": 12,
                                    "columns": [
                                        {"name": "col1", "datatype": "varchar"}
                                    ],
                      }
                            ],
                        }
                    ],
      }
            ],
        }
    ]
}
