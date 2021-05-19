from lithops import Storage
import json
import time
import lithops
from lithops.multiprocessing import Pool

def f(data):
    review = json.loads(data)
    return(review['reviewText'])

def readData(dic):
    storage = Storage()
    data = storage.get_object('analysis.data', 'rawdata/Automotive_5.json').decode()
    data = data.replace("}{","}\n{").splitlines()
    with Pool() as pool:
        result = pool.map(f, data)
    return result
    
if __name__ == '__main__':
    with lithops.FunctionExecutor() as fexec:
        fexec.call_async(readData, None)
        print(fexec.get_result())
