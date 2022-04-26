# Run:
# hadoop fs -rm -r hdfs:///files/hadoop_test_out
# python3 hbase_insert.py --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar -r hadoop hdfs:///files/hadoop_test --output-dir hdfs:///files/hadoop_test_out --no-cat-output

# run big: python3 hbase_insert.py --table hbase_insert_1 --hadoop-streaming-jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar -r hadoop hdfs:///files/hadoop_test --output-dir hdfs:///files/hbinsert --files hbase_connector.py

intermediate_buffer_size = 1000

import sys
sys.path.append('/home/ubuntu/.local/lib/python3.8/site-packages')

from mrjob.job import MRJob
import mrjob
import mrjob.protocol
import json
import pickle
import base64

import datasketch as ds
import hbase_connector

class HbaseInsertJob(MRJob):
    INPUT_PROTOCOL = mrjob.protocol.BytesValueProtocol
  
    def configure_args(self):
        super(HbaseInsertJob, self).configure_args()
        self.add_passthru_arg('--table', type=str, default="default_table", help="The hbase table prefix to insert data into")

    def mapper_init(self):
        self.pool = hbase_connector.HbaseConnection(host="localhost", port=9090)
        if self.options.table != None:
            table = self.options.table.encode('utf8')
        else:
            table = b'default_table'

        self.lsh = ds.lsh.MinHashLSH(threshold=0.8, num_perm=128, storage_config={'type': 'hbase', 'basename': table, 'hbase_pool': self.pool}, prepickle=True)
        self.buffer = []

    def insert(self):
        if len(self.buffer) <= 0:
            return

        def create_hash(string, num_perm=128):
            mh2 = ds.MinHash(num_perm=num_perm)
            for i, c in enumerate(string):
                mh2.update(i.to_bytes(8, byteorder='little') + c.encode('utf8'))
            return mh2

        
        
        with self.lsh.insertion_session() as sess:
            for key, value in self.buffer:
                sess.insert((key, value), create_hash(value, 128), check_duplication=False)
        self.buffer.clear()

    def mapper_final(self):
        self.insert()

    def mapper(self, _, value):
        data = value.decode('utf8')
        byt = base64.b64decode(data) 
        obj = pickle.loads(byt)
        
        self.buffer.append(obj)
        if len(self.buffer) > intermediate_buffer_size:
            self.insert()


if __name__ == "__main__":
    HbaseInsertJob.run()
