## Support files:

* `hbase_thrift/` - Generated HBase bindings for python.
    Generated from: https://github.com/apache/hbase/blob/master/hbase-thrift/src/main/resources/org/apache/hadoop/hbase/thrift2/hbase.thrift
    With: `thrift --gen py hbase.thrift`

* `hbase_connector.py` - HBase thrift2 connector & datasketch hbase bindings
    Usage: ```python
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
1. [SRA Toolkit](https://github.com/ncbi/sra-tools) - `fasterq-dump.3.0.0 --fasta `
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
