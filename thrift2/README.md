# thrift2
how to use this code is simple, just read it.
this is just a simple example how to connect hbase server with python.


### something need to know

1. hbase.thrift is in the hbase source code, define the interface. 
    the path like this: `hbase-1.0.1.1/hbase-thrift/src/main/resources/org/apache/hadoop/hbase/thrift2`
2. the code in hbase direction generate by thrift
    like this: `thrift --gen py hbase.thrift`
    
so if you use other hbase version, use up cmd to generate this code we need use.
