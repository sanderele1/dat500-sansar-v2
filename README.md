## Support files:

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
    

## Pipeline:

Files are implicitly assumed to be uploaded to hdfs if there is no step producing them.
We explicitly mention uploading `SRR15404285.fasta`, as you must run the sra-tools first. (Or just find a fasta file version)
These are also the cleaned up brethren of the actual files used to run the job for our presentation. We have not re-run everything after cleanup, so in-case there are errors, you may take a look at the original files in the old git repo.
> Warning: Use adequate protection, such as an industrial grade face shield or protective goggles, when opening the original git repository. We are not responsible for physical, nor emotional damage caused the chaos of files.
[The old (raw) git repository](https://github.com/sanderele1/dat500-project).

For your convenience, we have gathered all of the input and output files in a pre-assembled approx. 3GB `.tar.gz` file available [here (azure blob storage)](https://distributed.blob.core.windows.net/public/DAT500_blobs.tar.gz?sv=2020-10-02&st=2022-04-26T16%3A14%3A29Z&se=2023-04-27T16%3A14%3A00Z&sr=b&sp=r&sig=nBsI%2Bhw%2BrIchhbMlcjtE1Rdvp6OjqumhsIe0otQk6j8%3D) (available untill 2023, or whenever the project is no longer relevant. whichever is shorter).

### Sources:
* `assembledASM694v2` - NCBI: https://www.ncbi.nlm.nih.gov/assembly/GCF_000006945.2/
* `SRR15404285.sra` - NCBI: https://www.ncbi.nlm.nih.gov/sra/SRR15404285

### Building the fuzzy index
1. `sliding-window.ipynb` - Spark
    * Inputs: `hdfs:///files/salmonella/assembledASM694v2`
    * Outputs: `hdfs:///files/salmonella/window`
2. `convert-spark-hadoop-window.ipynb` - Spark
    * Inputs: `hdfs:///files/salmonella/window`
    * Outputs: `hdfs:///files/salmonella/window.b64pickled`
3. `hbase_insert.py` - Hadoop
    * Inputs: `hdfs:///files/salmonella/window.b64pickled`
    * Outputs: `<multiple hbase tables>: 'hbase_salmonella_pos_prefix_8`

### Querying for candidates, pre-alignment, filtering and read alignment
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
    
    
    
#### `sliding-window.ipynb` - Spark
This job computes a sliding window over the reference genome (assembledASM694v2), where the width of the window is the width of the sample reads (SRR15404285.fasta).

As each node does not hold the entire genome in memory at once, we do this on two stages, where we offset the second stage to compute sliding windows where the first one had memory borders.

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

### `convert-spark-hadoop-window.ipynb` - Spark
A simple job which just converts from Spark's sequence format, to base 64 encoded pickled objects, which hadoop understands easily.

We just pickle the whole object (key and value), base 64 encode it, and save each object as a line in a textfile using `RDD.saveAsTextFile()`.



## Cluster setup

We had a cluster setup, consisting of:
1. 1x namenode (1 vcpu, 2GB ram, 20GB disk, each)
2. 4x datanode (4 vcpu, 8GB ram, 80GB disk, each)

We would not go lower than this, especially not on the namenode. Whilst using hbase and spark, we were sitting around 95% of RAM.
This may be alleviated by not including the namenode as a HBase master. But if you want to run things as the hadoop history server to view past logs, and other extensions, look into atleast 4GB on the namenode.
We felt limited by our 2GB, as we could not use VS Code for development (it ate around 600MB), and were stuck with lighter weight alternatives such as jupyter-lab, or vi. As such, we strongly reccomend increasing the namenodes memory to 4GB.

On the datanodes, we're stuck with a bit of a dilemma. If we run just hadoop/spark, it's fine. But with HBase, problems start appearing. If you do not configure a memory limit for HBase, nor hadoop/spark, you quickly run into issues running out of memory. If you configure HBase to use 50% of the ram, and hadoop/spark the other 50%, you're missing out on performance for all the jobs not requiring HBase to be running.  Limiting the memory available to HBase may also impact write/read performance, although other than running out of memory, we did not profile this comparatively. You could shut down your hbase cluster between jobs when not in use, and individually limit hadoop/spark memory usage when hbase is running, but you loose your HBase cache. Had memory been doubled, to 16GB, it probably would have been fine with both (assuming you configure a max memory limit, otherwise they may fight eachother), as the pressure from other services consuming memory would have lessened.
That being said, 8GB was entierly doable, and we could use 8GB datanodes again.

