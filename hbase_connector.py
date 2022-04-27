import datasketch as ds
import struct
import pickle
import base64
import time
import random
import pickle

import thrift as t
import hbase_thrift.hbase as hb

from hbase_thrift.hbase import THBaseService
from hbase_thrift.hbase.ttypes import *

from thrift.transport import *
from thrift.protocol import *







class HbaseConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        
        self._socket = None
        self._client = None
        
    def _create_transport(self):
        if self._socket != None and self._socket.isOpen():
            try:
                self._socket.close()
            except:
                pass
        
        self._socket = TSocket.TSocket(self.host, self.port)
        self._transport = TTransport.TBufferedTransport(self._socket)
        self._protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
        self._client = THBaseService.Client(self._protocol)
        self._transport.open()
        
    def __enter__(self):
        if self._client == None or not self._socket.isOpen():
            self._create_transport()
        
        return self._client
    
    def __exit__(self, _, __, ___):
        pass
    
    
    
    

def hbase_safe_b64_encode(data):
    return base64.b64encode(data).decode('utf8').replace('=', '').encode('utf8')

class HBaseDictListStorage(ds.storage.OrderedStorage):
    # Order may be important in insertion in this storage object, but I'm not sure. Let's assume not for now, and get burned by it later
    def __init__(self, config, name):
        self._name = name
        self._table = hbase_safe_b64_encode(name)
        self._pool = config['hbase_pool']
        
        self.buffer = []
        
        needs_create_table = True
        
        retries = 0
        max_retries = 10
    
        while needs_create_table and retries < max_retries:
            try:
                with self._pool as c:
                    needs_create_table = not c.tableExists(TTableName(qualifier=self._table))
            except TIOError as e:
                message = e.message.decode('utf8')
                if not message.startswith('org.apache.hadoop.hbase.TableNotFoundException'):
                    raise e
    
            if needs_create_table:
                ###print(f"Finished sleeping, attempting to create table ({retries}/{max_retries} retries): {self._name}")
                
                try:
                    with self._pool as c:
                        # Create table here:
                        key_family = TColumnFamilyDescriptor(name=b'key', bloomnFilterType=TBloomFilterType.ROWCOL)
                        value_family = TColumnFamilyDescriptor(name=b'value', bloomnFilterType=TBloomFilterType.ROWCOL)
                        name = TTableName(qualifier=self._table)
                        newTable = TTableDescriptor(tableName=name, columns=[key_family, value_family])
                        c.createTable(newTable, None)
                        needs_create_table = False
                        ###print(f"Successfully create table: {self._name}")
                except BaseException as e:
                    retries += 1
                    ###print(f"Failed to create table: {self._name} with exception: {e}")
                    sleep_time = random.uniform(0.01, 2)
                    ###print(f"Need to create table:  {self._name} ({self._table}), sleeping {sleep_time} seconds")
                    time.sleep(sleep_time)
                    #raise e
                
        if needs_create_table:
            raise ValueError(f"Failed to create table {self._name} with {retries} retries")
        

    def keys(self):
        raise ValueError('Not implemented')

    def get(self, key):
        with self._pool as c:
            r = c.get(self._table, TGet(row=key))
            return [q.qualifier for q in r.columnValues]

    def remove(self, *keys):
        raise ValueError('Not implemented')

    def remove_val(self, key, val):
        raise ValueError('Not implemented')

    def empty_buffer(self):
        # Used to execute large batch
        if len(self.buffer) > 0:
            self._insert(self.buffer)
            self.buffer.clear()
    
    def _insert(self, values):
            with self._pool as c:
                c.putMultiple(self._table, values)
                
                
        
    def insert(self, key, *vals, **kwargs):
        # Needs implementation
        # Should check: kwargs['buffer'], if true, buffer untill empty_buffer is called
        
        cols = [TColumnValue(family=b'value', qualifier=v, value=b'') for v in vals]
        put = TPut(row=key, columnValues=cols, durability=TDurability.SKIP_WAL)
        
        if kwargs['buffer'] and kwargs['buffer'] == True:
            self.buffer.append(put)
        else:
            self._insert([put])
        

    def size(self):
        raise ValueError('Not implemented')

    def itemcounts(self, **kwargs):
        raise ValueError('Not implemented')
        
    def has_key(self, key):
        # Needs implementation
        with self._pool as c:
            return c.exists(self._table, TGet(row=key))
    
    
    
    
        
        
class HBaseDictSetStorage(ds.storage.UnorderedStorage, HBaseDictListStorage):
    '''This is a wrapper class around ``defaultdict(set)`` enabling
    it to support an API consistent with `Storage`
    '''
    def __init__(self, config, name=None):
        HBaseDictListStorage.__init__(self, config, name=name)

    def get(self, key):
        return set(HBaseDictListStorage.get(self, key))

    def insert(self, key, *vals, **kwargs):
        HBaseDictListStorage.insert(self, key, *vals, **kwargs)
        
        
        
        
def hbase_ordered_storage(config, name=None):
    #print(f"Overriden ordered storage ran with config: {config}")
    tp = config['type']
    if tp == 'hbase':
        return HBaseDictListStorage(config, name=name)
    else:
        return ds.storage.ordered_storage(config, name=name)


def hbase_unordered_storage(config, name=None):
    #print(f"Overriden unordered storage ran with config: {config}")
    tp = config['type']
    if tp == 'hbase':
        return HBaseDictSetStorage(config, name=name)
    else:
        return ds.storage.unordered_storage(config, name=name)
    
    
    
    
    
def override_lsh__init__(self, threshold=0.9, num_perm=128, weights=(0.5, 0.5),
                 params=None, storage_config=None, prepickle=None, hashfunc=None):
        storage_config = {'type': 'dict'} if not storage_config else storage_config
        self._buffer_size = 50000
        if threshold > 1.0 or threshold < 0.0:
            raise ValueError("threshold must be in [0.0, 1.0]")
        if num_perm < 2:
            raise ValueError("Too few permutation functions")
        if any(w < 0.0 or w > 1.0 for w in weights):
            raise ValueError("Weight must be in [0.0, 1.0]")
        if sum(weights) != 1.0:
            raise ValueError("Weights must sum to 1.0")
        self.h = num_perm
    
        
        
        if params is not None:
            self.b, self.r = params
            if self.b * self.r > num_perm:
                raise ValueError("The product of b and r in params is "
                        "{} * {} = {} -- it must be less than num_perm {}. "
                        "Did you forget to specify num_perm?".format(
                            self.b, self.r, self.b*self.r, num_perm))
        else:
            false_positive_weight, false_negative_weight = weights
            self.b, self.r = ds.lsh._optimal_param(threshold, num_perm,
                    false_positive_weight, false_negative_weight)

        self.prepickle = storage_config['type'] == 'redis' if prepickle is None else prepickle
        
        
        self.hashfunc = hashfunc
        if hashfunc:
            self._H = self._hashed_byteswap
        else:
            self._H = self._byteswap

        basename = storage_config.get('basename', ds.storage._random_name(11))
        self.hashtables = [
            hbase_unordered_storage(storage_config, name=b''.join([basename, b'_bucket_', struct.pack('>H', i)]))
            for i in range(self.b)]
        self.hashranges = [(i*self.r, (i+1)*self.r) for i in range(self.b)]
        self.keys = hbase_ordered_storage(storage_config, name=b''.join([basename, b'_keys']))
        
ds.lsh.MinHashLSH.__init__ = override_lsh__init__