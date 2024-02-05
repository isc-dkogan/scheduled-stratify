#!/bin/bash

echo ${pwd}
sqlite3 tests/data/test.db < tests/data/position_create_insert.sql
sqlite3 tests/data/test.db < tests/data/security_master_create_insert.sql
