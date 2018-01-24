# Test 1: Many tasks.

import numpy as np
import ray
import time

ray.init(redis_address="169.229.49.45:6379", object_id_seed=1234)

@ray.remote
def f():
    return 1

time.sleep(1)

N = 10000  # Number of tasks. Vary this number.
K = 10  # Number of stages. Vary this number.

for _ in range(K):
    ray.get([f.remote() for _ in range(N)])

tt = ray.global_state.task_table()
fh = open("obj_task_map_eb1.csv", "w")

for k in tt:
    fh.write(tt[k]['TaskSpec']['TaskID'])
    fh.write(",")
    fh.write(str(tt[k]['TaskSpec']['ReturnObjectIDs']))
    fh.write(",")
    fh.write(str(tt[k]['TaskSpec']['Args']))
    fh.write("\n")

fh.close();
