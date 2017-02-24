Example HBase usage:

The ADAM HBase module currently only supports the CDH distribution of HBase, and thus needs to be run on
the a Cloudera CDH platform


Be sure `SPARK_HOME` env variable points to your Spark 1.6.x Spark installation
The Spark/HBase connector does not currently support Spark 2.

Set `HADOOP_CONF_DIR` env variable, in my case:
```
export HADOOP_CONF_DIR=/etc/hadoop/conf
```

Launch adam-shell, including `--jars` which make HBase module classes visible
```
./bin/adam-shell --jars adam-assembly/target/adam_2.10-0.21.1-SNAPSHOT.jar,adam-hbase/target/adam-hbase_2.10-0.21.1-SNAPSHOT.jar
  \ --master yarn-client --num-executors 5 --executor-cores 2 --executor-memory 20g
```

At the adam-shell command prompt run the follow commands:

Imports
```
import org.bdgenomics.adam.hbase.HBaseFunctions
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMContext
```

Create data access object used to manage the HBase connection
```
val myDAO = new HBaseFunctions.HBaseDataAccessObject(sc)
```

Create HBase table, in this example named `jp_02232018_v10`
```
HBaseFunctions.createHBaseGenotypeTable(myDAO, "jp_02232018_v10")
```

Create a Spark RDD based on VCF file
```
val x2 = sc.loadVcf("hdfs://amp-bdg-master.amplab.net:8020/user/jpaschall/chr22_testdata/HG00096.head.2000.vcf")
```

Save Variant data to HBase
```
HBaseFunctions.saveVariantContextRDDToHBaseBulk(myDAO,x2,"jp_02232018_v10","my_seq_dict","hdfs://amp-bdg-master.amplab.net:8020/user/jpaschall/temp_02222018_v10")
```

Retrieve Variant data from HBase
```
val t1 = HBaseFunctions.loadGenotypesFromHBase(myDAO,"jp_02232018_v10",List("HG00096"),"my_seq_dict")
```

