# Support files:

* `hbase_thrift/` - Generated HBase bindings for python.
    Generated from: https://github.com/apache/hbase/blob/master/hbase-thrift/src/main/resources/org/apache/hadoop/hbase/thrift2/hbase.thrift
    With: `thrift --gen py hbase.thrift`

* `hbase_connector.py` - HBase thrift2 connector & datasketch hbase bindings
    Usage: 
    ```python
    import datasketch as ds
    import hbase_connector as hb # IMPORTANT: Must import this library after datasketch
    # A refactor should probably be made to use a '.init()' function to inject the required functions into datasketch
    # Similar API interface to findspark, hopefully it will avoid confusion.
    # Also note: when using the connector in datasketch for the first time for a table, it may take a while to execute. This is due to it creating tables inside hbase with the specified prefix (encoded with base 64). Have some patience :)
    ```
    
    Demos:
    * `thrift2.ipynb` - HBase thrift2 connector demo
    * `ds_thrift2.ipynb` - Datasketch LSH HBase binding 
        
* `GenASM/` - Custom python binding for [GenASM](https://github.com/CMU-SAFARI/GenASM)
    Note: not all contents of `genasm_aligner.c` is own work. Most of it is from the GenASM project. We only buildt the python bindings.

    You can build the project by looking into `build.sh`, or in short:
    ```
    python3 setup.py build
    # And then copy *.so file to the root directory (where the python files are)
    ```
    
* `gasm.cpython-38-x86_64-linux-gnu.so` - Pre-buildt binaries for the custom GenASM bindings.
    

# Pipeline:

Files are implicitly assumed to be uploaded to hdfs if there is no step producing them.
We explicitly mention uploading `SRR15404285.fasta`, as you must run the sra-tools first. (Or just find a fasta file version)
These are also the cleaned up brethren of the actual files used to run the job for our presentation. We have not re-run everything after cleanup, so in-case there are errors, you may take a look at the original files in the old git repo.
> Warning: Use adequate protection, such as an industrial grade face shield or protective goggles, when opening the original git repository. We are not responsible for physical, nor emotional damage caused the chaos of files.
[The old (raw) git repository](https://github.com/sanderele1/dat500-project).

For your convenience, we have gathered all of the input and output files in a pre-assembled approx. 3GB `.tar.gz` file available [here (azure blob storage)](https://distributed.blob.core.windows.net/public/DAT500_blobs.tar.gz?sv=2020-10-02&st=2022-04-26T16%3A14%3A29Z&se=2023-04-27T16%3A14%3A00Z&sr=b&sp=r&sig=nBsI%2Bhw%2BrIchhbMlcjtE1Rdvp6OjqumhsIe0otQk6j8%3D) (available untill 2023, or whenever the project is no longer relevant. whichever is shorter).

## Sources:
* `assembledASM694v2` - NCBI: https://www.ncbi.nlm.nih.gov/assembly/GCF_000006945.2/
* `SRR15404285.sra` - NCBI: https://www.ncbi.nlm.nih.gov/sra/SRR15404285

## Building the fuzzy index
1. `sliding-window.ipynb` - Spark
    * Inputs: `hdfs:///files/salmonella/assembledASM694v2`
    * Outputs: `hdfs:///files/salmonella/window`
2. `convert-spark-hadoop-window.ipynb` - Spark
    * Inputs: `hdfs:///files/salmonella/window`
    * Outputs: `hdfs:///files/salmonella/window.b64pickled`
3. `hbase_insert.py` - Hadoop
    * Inputs: `hdfs:///files/salmonella/window.b64pickled`
    * Outputs: `<multiple hbase tables>: 'hbase_salmonella_pos_prefix_8`

## Querying for candidates, pre-alignment, filtering and read alignment
1. [SRA Toolkit](https://github.com/ncbi/sra-tools) - `fasterq-dump.3.0.0 --fasta SRR15404285.sra`
    * Inputs: (local fs) `SRR15404285.sra`
    * Outputs: (local fs) `SRR15404285.fasta`
2. Hadoop FS - `hadoop fs -put "SRR15404285.fasta" "hdfs:///files/salmonella/SRR15404285.fasta"`
    * Inputs: (local fs) `SRR15404285.fasta`
    * Outputs: `hdfs:///files/salmonella/SRR15404285.fasta`
1. `preprocess-reads.ipynb` - Spark
    * Inputs: `hdfs:///files/salmonella/SRR15404285.fasta`
    * Outputs: `hdfs:///files/salmonella/SRR15404285.pickleb64.320`
2. `mrjob_ass_safe.py` - Hadoop
    * Inputs: `<multiple hbase tables>: 'hbase_salmonella_pos_prefix_8`
    * Inputs: `hdfs:///files/salmonella/SRR15404285.pickleb64.320`
    * Outputs: `hdfs:///files/salmonella/matches_v8`
3. `write-assembled-nohbase.py` - Hadoop
    * Inputs: `hdfs:///files/salmonella/matches_v8`
    * Outputs: `hdfs:///files/salmonella/grouped_positions`
4. `re-assemble-grouped-positions.ipynb` - Spark
    * Inputs: `hdfs:///files/salmonella/grouped_positions`
    * Outputs: `hdfs:///files/salmonella/assembly_reconstructed`

### Analysis
1. `assembly-inspection.ipynb` - Spark
    * Inputs: `hdfs:///files/salmonella/assembly_reconstructed`
    * Inputs: `hdfs:///files/salmonella/assembledASM694v2`
    * Outputs: `<human interaction/none>`
    
### Spark v. Hadoop

> Note: When we are discussing hadoop, what we really mean is MRJob on Hadoop. We did not use Hadoop without MRJob, so keep that in mind. **So when we say Hadoop, we mean MRJob on Hadoop.**

After having worked with spark and hadoop, we decided to use spark where we could. It's much faster to prototype with, being designed with rapid iteration in mind, even supporting jupyter notebooks (which we love). Spark let us get the job done fast (both in time required to solve a problem, and execution time), being easy and quick to work with being the main benefit. We only used hadoop when we ran into memory issues when inserting, and reading from hbase. 

Hadoop also proved much easier to do abusive things with, like building a micro mapping framework inside MRJob in python. We did that due to thrift2 using blocking IO, to get a higher read throughput. It worked, but was no longer neccesary when we ran the job on the datanodes, having 4x more containers running on each, with lower round-trip time to hbase (each node locally hosted a hbase region server). You can view the remnants of that monstrosity in the old git repo, in the files: [`mrjob_ass.py`](https://github.com/sanderele1/dat500-project/blob/master/python/v2/mrjob_ass.py), [`mrjob_assembler.py`](https://github.com/sanderele1/dat500-project/blob/master/python/v2/mrjob_assembler.py), and [`assembler_perf.py`](https://github.com/sanderele1/dat500-project/blob/master/python/v2/assembler_perf.py). 

If you intend to use multiprocessing inside hadoop, ensure you check if anything you are running is using a logging library. Our thrift2 bindings were, and were attempting to log string to *stderr*, which caused exceptions due to *stderr* being opened in binary mode. You can get some really odd errors from this, as the error reporting mechanism causes the error.
To disable logging (to see if it is the issue), you may run:
```python
import logging
# ...
logging.disable(logging.CRITICAL)
```
*Remember, you may not be calling any logging, but a library you have imported may!*


When you are using spark, it is also important to note that the smallest unit spark will process in parallel is a partition. If you have 1000 cores, and 1 partition, only 1 core will be used. If you have fewer partitions that you have cores, or using operations that reduce the number of partitions, consider re-partitioning with [`.repartition(num_paritions)`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.repartition.html)
If you have your cluster running on all your datanodes, but only see a few cores being utilized and you correctly configured `spark.executor.instances` in your config, then too few partitions may be the issue.

You can easily configure the number of executors, and their memory limit individually in your python scripts or notebooks, for example like this:
```python
from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .master("yarn")
         .appName("your-brilliant-spark-appname")
         .config("spark.executor.instances", 16)
         .config("spark.executor.memory", "1536m")
         .getOrCreate())

sc = spark.sparkContext
# sc is if you want access to RDD's
```

### LSH Embedding format
When computing the LSH of our bases, we use the following python function:
```python
# import datasketch as ds # <- We assume you have this somewhere in your file
def create_hash(sequence):
    mh2 = ds.MinHash(num_perm=128)
    for i, c in enumerate(sequence):
        mh2.update(i.to_bytes(8, byteorder='little') + c.encode('utf8'))
    return mh2
```

We encode base position as a absolute embedding (from the perspective of the sample read)

LSH works on jaccardian set distance, and as such does not take element position into account, unless we tell it to.
Ie: `hello` == `olleh`, as they both contain the same letters (they're a set of the same thing)

***Absolute embeddings***

The function uses simple absolute embeddings, which means it just encodes the position of each base with the base.
So: `ATCG...` becomes `[(0, 'A'), (1, 'T'), (2, 'C'), ...]`

This is very quick to compute, and query. A single mutation will only cause a single change. (ie: if `(1, 'T')` changes to `(1, 'G')`, only a single element of the set is different).
This method struggles with insertions or deletions in the sequence, as if we for example do a insertion of `(1, 'G')` into position 1:
`[(0, 'A'), (1, 'T'), (2, 'C'), ...]` becomes `[(0, 'A'), (1, 'G'), (2, 'T'), (3, 'C'), ...]`. As you can see, ever element after `(1, 'A')` will be different.

In practice this is not neccesarily a big problem, as the sliding window covers all windows. Given enough reads, some read is probable to have the insertion/deletion be at the end, affecting few bases, and having a jaccardian difference less than the threshold.
LSH does not do read alignment, so if we get false positives that is absolutely ok. We just don't want to miss index hits for the matching reference. But we also do not want to get too many candidate results back (a candidate result is just another word for a match, we can get many matches, and so many candidates. We call it a candidate before we run GenASM to actually decide which candidates are matches or not)

Absolute embeddings is what we decided to use (due to the performance benefits, measured informally). We did not do a benchmark comparison, and leave it as a future exercise.

***Relative embeddings***

Another method, is to encode position relative to other elements. We can do this by just using a small sliding window.
So: `ATCG...` becomes `[('AT'), ('TC'), ('CG'), ...]`.
We can vary the window size to affect how stringent we are about the position.
This can handle insertions and deletions really well, as the insertion/deletion will only cause a mismatch of the area immediately round it.
However, this is slower to compute, and to query (As in general, there will be more matches).
We also have the problem that a single mutation will not cause multiple differences in the set.
Imagine if `T` in `ATCG...` flips to A, we get: `[('AA'), ('AC'), ('CG'), ...]`
Multiple elements of the set now differ.

### `sliding-window.ipynb` - Spark
This job computes a sliding window over the reference genome (assembledASM694v2), where the width of the window is the width of the sample reads (SRR15404285.fasta).

As each node does not hold the entire genome in memory at once, we do this in two stages, where we offset the second stage to compute sliding windows where the first one had memory borders.

1. Take the text:
    
    `Really long string, too big to fit on a single server`

2. Spark splits the text up into partitions:
     `Really long string, too` and ` big to fit on a single server`

3. We compute sliding windows for each partition individually:
    \[`Rea`, `eal`, ..., `too`\] and \[` bi`, `big`, ..., `ver`\]
    We're missing \[`oo `, `o b`\]
    
4. We repartition our data, and shuffle the data with a offset equal to the window size.
    `Really long string, too b` and `ig to fit on a single server`

5. We compute sliding windows again:
    \[`Rea`, `eal`, ..., `oo `, `o b`\] and \[` bi`, `big`, ..., `ver`\]

6. We union the results, and we have our window.

> Note: This could be made much more efficient by simply calculating the few border windows on the second pass, and not all of them again. You could then do a simple append, instead of a complicated join (as positions should be unique).

Example input: `['AGAGATTACGTCTGGTTGCAAGAGATCATGACAGGGGGAATTGGTTGAAAATAAATATATCGCCAGCAGCACATGAACAA']`

Example outputs (2 outputs): 
``
[(3532223, 'GCCACGCTATCGACGGTACCTTTTAATACCCGGTTGCTGCCAAGCGGCGTGATTTCGGCACGATATCCCGGACGC'),
 (3729573, 'AGCTCTTTGGTCTCTTTCGGGTTAAGGCCAGCCGCCGGTTCGTCGAGCATCAGAATTTCTGGCTGCGTCACCATG')]
``

### `convert-spark-hadoop-window.ipynb` - Spark
A simple job which just converts from Spark's sequence format, to base 64 encoded pickled objects, which hadoop understands easily.

We just pickle the whole object (key and value), base 64 encode it, and save each object as a line in a textfile using `RDD.saveAsTextFile()`.

Example input: `[(3006618, 'TCCTCGCGAATGGTCTGAACCTGGAGCGATGGTTCGCCCGCTTTTATCAGCACCTTTCCGGCGTGCCGGAAGTCG')]`

Example output: `[b'gASVVgAAAAAAAABKmuAtAIxLVENDVENHQ0dBQVRHR1RDVEdBQUNDVEdHQUdDR0FUR0dUVENHQ0NDR0NUVFRUQVRDQUdDQUNDVFRUQ0NHR0NHVEdDQ0dHQUFHVENHlIaULg==']`

### `hbase_insert.py` - Hadoop

This job computes LSH hashes of all of the windows, produced by `sliding-window.ipynb`, and inserts them into a HBase database.
HBase insertion happens in parallel, on hadoop. Please refer to the section [LSH Embedding format](#lsh-embedding-format) for how LSH is calculated.

We designed the hadoop table format to be idempotent when inserting, in case of errors causing partial insertion. We are able to restart the jobs that were not completed, and re-run them. If something get's inserted twice, it will simply overwrite the old value (essentially doing nothing).)

HBase doesn't have a schema per-se, you only need to decide upon "column-families" when creating a table. You can then re-use these column-families for as many columns in a row you want. Every row can have different columns, and are independent. It's with the column families you can decide things like bloom filters, version history, etc...
So to insert a row, you need a row key, and a list of columns (which consists of a column family, a column qualifier, and a value).
We use the fact that each row can be looked upon like a dictionary in our table design.

Datasketch requires two types of storage, a dictionary of lists, and a dictionary of sets.
We need the ability to insert values into a given key, and to look up the values of that key, for the dictionary of lists.
This is implemented in HBase by giving the key as the row-key, and the values are encoded as column qualifiers. Column value is left empty. If the same key-value pair is inserted multiple times, it will just overwrite the last one without issue.
Due to the idempotent design of insertion, we can skip the write-ahead log, as if a container fails, we can just re-run that container.

The dictionary of sets was implemented ontop of the dictionary of lists.

For both of these classes, see: `HBaseDictListStorage` and `HBaseDictSetStorage` in `hbase_connector.py`.
> Note: We did not implement all of the functionality provided by datasketch, like deleting values. We implemented only what we needed to perform our required tasks of insertion, and querying. Although this could be fairly easily implemented.

For our column families, we added a bloom filter for our rows and columns (for faster lookups). And in our infinite wisdom, we forgot to enable the filter when running the insert job for the report (oops). It should be enabled now for future executions.

### `preprocess-reads.ipynb` - Spark
For why we use Spark for preprocessing, see section: [Spark v. Hadoop](#spark-v-hadoop)

A simple job, which does preprocessing on `hdfs:///files/salmonella/SRR15404285.fasta`, and produces `hdfs:///files/salmonella/SRR15404285.pickleb64.320`. It simply extracts every read as a string, along with the index of that read into the `SRR15404285.fasta` file, making each read unique (so we could tract matches to reads later, if needed). We call that index the "read index". The output is repartitioned (we had 320 partitions, hence `.320` in the filename. To see why 320, see section [`mrjob_ass_safe.py` - Hadoop](#mrjob_ass_safepy---hadoop)), pickled, base64 encoded, then saved as a text file (with each line being one read object).

Example input: `[(0, 'TGCCGNCCTGAGCGAAAGCCTGCTGGAAGAAGTAGCTTCGCTGGTGGAATGGCCGGTGGTATTGACGGCGAAATT')]`

Example output: `[b'gASVUwAAAAAAAABLAIxLVEdDQ0dOQ0NUR0FHQ0dBQUFHQ0NUR0NUR0dBQUdBQUdUQUdDVFRDR0NUR0dUR0dBQVRHR0NDR0dUR0dUQVRUR0FDR0dDR0FBQVRUlIaULg==']`

### `mrjob_ass_safe.py` - Hadoop
The job that queries the HBase databse with the sample reads, and outputs whole read-aligned matches (ie: not individual bases, but whole sequences).

It runs the following steps in the mapper (which runs for every sample read):

1. Decode input (base64decode, unpickle)
2. Query HBase for LSH candidates
3. Compute GenASM edit distance for all candidates
4. Find the smallest edit distance
5. Yield read_index, read, [all candidates with an edit distance equal the smallest edit distance found in step 4.]

read_index is the unique read index described in section [`preprocess-reads.ipynb` - Spark](#preprocess-readsipynb---spark)

We had some issues running this job, where some containers would somehow end up in a tigh-loop inside mapper_init (or atleast before mapper was evaluated). We added logging to *stderr* inside mapper, and on these containers specifically, it was never called. This would only happen occasionally, and we got sufficient usable output that we were able to reconstruct most of the DNA (approx. 98%). We are still not sure why this happened, or what caused it. So please keep this in mind if you decide to run this job. We got around the issue by manually killing the containers using htop that were misbehaving. You can tell which containers they are, as they use all cpu resources avaiable to them, and they do not exit.



### `write-assembled-nohbase.py` - Hadoop

This is a post-processing job, that converts the whole sample reads from [`mrjob_ass_safe.py`](#mrjob_ass_safepy---hadoop) into per-base reads.

**Mapper**

The mapper takes a list of full sample reads, with the respective index matches. It then yields once for every calculated base position in those reads. The base position is the key, and all computed candidate bases as the value.
It calculates the pase position, by taking the index of the base in the sample read, and adding the index of the first base in the associated match, for all the matches.

Pseudocode of the mapper (for the actual implementation, see `write-assembled-nohbase.py`):

```python
    # Pseudocode, see write-assembled-nohbase.py for implementation
    def mapper(self, _, value):
        read_value, raw_matches = value
        for raw_match in raw_matches:
            match_index, match_value, match_comparison = raw_match

            for i, base in enumerate(read_value):
                base_position = match_index + i
                yield f"{base_position}", [base]
```

**Combiner & Reducer**

The combiner and reducer both group the candidate bases into a single list, essentially combining the "votes" from the mapper. Once the reducer is finished running, we'll have all our base positions as our keys, and all matching 

Actual code for the combiner and reducer:

```python
def combiner(self, key, values):
    # https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-a-list-of-lists
    yield key, [item for sublist in values for item in sublist]

def reducer(self, key, values):
    # https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-a-list-of-lists
    yield key, [item for sublist in values for item in sublist]
```



### `re-assemble-grouped-positions.ipynb` - Spark
Takes the output of `write-assembled-nohbase.py` and makes it easier to consume.

 It finds the majority base for every base position, and yeilds that base for that position, also including vote percentage for that base, votes for that base, and all votes (passing it along from `write-assembled-nohbase.py`, in case you want to do something with it).

 Example input: `['"100000"\t["A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "A"]']`

 Example output: `[(100000, ('A', 1.0, 11, [('A', 11)]))]`



## Results for `SRR15404285.sra` and `assembledASM694v2` (index)
You can find the result analysis in: `assembly-inspection.ipynb`.



## Cluster setup

We had a cluster setup, consisting of:
1. 1x namenode (1 vcpu, 2GB ram, 20GB disk, each)
2. 4x datanode (4 vcpu, 8GB ram, 80GB disk, each)

We had no particular reason for choosing this cluster setup, other than being allocated 17 cores, and being recommended that 1vcpu and 2GB of ram was more than sufficient for a namenode.
We also needed at least 3 nodes due to the project description. With the openstack vm skews available to us, it was the most obvious setup (not including 8x 2vcpu, or 16x 1vcpu, but os and application overhead would potentially become serious issues with these sizes).

We would not go lower than this, especially not on the namenode. Whilst using hbase and spark, we were sitting around 95% of RAM.
This may be alleviated by not including the namenode as a HBase master. But if you want to run things as the hadoop history server to view past logs, and other extensions, look into atleast 4GB on the namenode.
We felt limited by our 2GB, as we could not use VS Code for development (it ate around 600MB), and were stuck with lighter weight alternatives such as jupyter-lab, or vi. As such, we strongly reccomend increasing the namenodes memory to 4GB.

On the datanodes, we're stuck with a bit of a dilemma. If we run just hadoop/spark, it's fine. But with HBase, problems start appearing. If you do not configure a memory limit for HBase, nor hadoop/spark, you quickly run into issues running out of memory. If you configure HBase to use 50% of the ram, and hadoop/spark the other 50%, you're missing out on performance for all the jobs not requiring HBase to be running.  Limiting the memory available to HBase may also impact write/read performance, although other than running out of memory, we did not profile this comparatively. You could shut down your hbase cluster between jobs when not in use, and individually limit hadoop/spark memory usage when hbase is running, but you loose your HBase cache. Had memory been doubled, to 16GB, it probably would have been fine with both (assuming you configure a max memory limit, otherwise they may fight eachother), as the pressure from other services consuming memory would have lessened.
That being said, 8GB was entierly doable, and we could use 8GB datanodes again.

