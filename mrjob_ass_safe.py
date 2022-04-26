import sys
sys.path.append('/home/ubuntu/.local/lib/python3.8/site-packages')
sys.path.append('/home/ubuntu/GenASM/build/lib.linux-x86_64-3.8') # For the homebrew GenASM bindings in C. These need to be added to the datanodes too.
sys.path.append('./')

import json
import pickle
import base64

import gasm # Homebrew package
import datasketch as ds

# Import logging is critical, as TSocket tries to log stuff when there is an error, but this fails in mrjob multiprocess as a binary stderr is assumed.
import logging
import hbase_connector

import multiprocessing as mp
from multiprocessing import Process, Queue, Pipe, Pool
import time
import gc

# These are the logging namespaces
# ["redis.cluster", "redis", "concurrent.futures", "concurrent", "asyncio", "thrift.transport.TSocket", "thrift.transport", "thrift", "mrjob.util", "mrjob", "mrjob.conf", "mrjob.parse", "mrjob.options", "mrjob.compat", "mrjob.fs.base", "mrjob.fs", "mrjob.fs.composite", "mrjob.fs.local", "mrjob.setup", "mrjob.step", "mrjob.runner", "mrjob.job", "__main__", "mrjob.sim", "mrjob.inline"]







from mrjob.job import MRJob

    
def create_hash(string, nperm=128):
    mh2 = ds.MinHash(num_perm=nperm)
    for i, c in enumerate(string):
        mh2.update(i.to_bytes(8, byteorder='little') + c.encode('utf8'))
    return mh2

import mrjob.protocol

class MRJobAssemblySafe(MRJob):
    
    OUTPUT_PROTOCOL = mrjob.protocol.PickleProtocol
    
    def mapper_init(self):
        #logging.disable(logging.CRITICAL)
    
        self._pool = hbase_connector.HbaseConnection(host="localhost", port=9090)
        self._lsh = ds.lsh.MinHashLSH(threshold=0.8, num_perm=128, storage_config={'type': 'hbase', 'basename': b'hbase_salmonella_pos_prefix_8', 'hbase_pool': self._pool}, prepickle=True)
        #self.buffer = []
        
    def mapper(self, _, value):
        sys.stderr.write(f"Processing: {value}")
        data_b = base64.b64decode(value)
        data_p = pickle.loads(data_b)
        
        read_number, read = data_p
        
        
        
        candidate_matches = self._lsh.query(create_hash(read))
        qualities = [(candidate, gasm.gasmAlignment(read, candidate[1], 20, 20, 4, 5, 1)) for candidate in candidate_matches]
        qualities = list(filter(lambda x: x[1][2] != '', qualities))
        if len(qualities) > 0:            
            ed = min(map(lambda x: x[1][0], qualities))
            filtered = filter(lambda x: x[1][0] == ed, qualities)
            
            yield None, ((read_number, read), list(filtered))
        sys.stderr.write(f"Finished: {value}")

if __name__ == '__main__':
    #mp.set_start_method('spawn')
    MRJobAssemblySafe.run()
    
# python3 mrjob_ass_safe.py -r inline testdata.hadoop --files hbase_connector.py,gasm.cpython-38-x86_64-linux-gnu.so
# python3 mrjob_ass_safe.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar -r hadoop hdfs:///files/testdata.hadoop --files hbase_connector.py,gasm.cpython-38-x86_64-linux-gnu.so

# BIG BOI: mrjob_ass_safe.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar -r hadoop hdfs:///files/salmonella/SRR15404285.pickleb64.320 --files hbase_connector.py,gasm.cpython-38-x86_64-linux-gnu.so --output-dir hdfs:///files/salmonella/matches_v8 -Dmapreduce.task.timeout=3600000

