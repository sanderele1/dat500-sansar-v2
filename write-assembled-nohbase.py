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
import mrjob.protocol

class CanMrJobDoThis(MRJob):
    
    INPUT_PROTOCOL = mrjob.protocol.PickleProtocol
    INTERNAL_PROTOCOL = mrjob.protocol.PickleProtocol
    OUTPUT_PROTOCOL = mrjob.protocol.JSONProtocol
    
    def mapper_init(self):
        pass
    
    def mapper(self, _, value):
        raw_read = value[0]
        raw_matches = value[1]
        
        read_index = raw_read[0]
        read_value = raw_read[1]
        for raw_match in raw_matches:
            match_index = raw_match[0][0]
            match_value = raw_match[0][1]
            
            # match_comparison is (ed, score, cigar, cigarv2, ...)
            # From our mrjob_ass.py, all candidates we get have the same edit score
            match_comparison = raw_match[1]

            for i, base in enumerate(read_value):
                base_position = match_index + i
                yield f"{base_position}", [base]
                
    def combiner(self, key, values):
        # https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-a-list-of-lists
        yield key, [item for sublist in values for item in sublist]
    
    def reducer(self, key, values):
        # https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-a-list-of-lists
        yield key, [item for sublist in values for item in sublist]
    
if __name__ == '__main__':
    CanMrJobDoThis.run()
    
# python3 write-assembled-nohbase.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar -r hadoop hdfs:///files/salmonella/matches_v8 --files hbase_connector.py,gasm.cpython-38-x86_64-linux-gnu.so --output-dir hdfs:///files/salmonella/grouped_positions