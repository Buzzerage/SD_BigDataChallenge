from lithops import Storage
import json
import time
import lithops
from lithops.multiprocessing import Pool    

if __name__ == '__main__':

    obj_key = 'filename'
    storage = Storage()
    obj_id = storage.put_cloudobject('', 'analysis.data', obj_key)
    print(obj_id)

