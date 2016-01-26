# thrift
how to use this code is simple, just read it.
this is just a simple example how to connect hbase server with python.


### something need to know

1. Hbase.thrift is in the hbase source code, define the interface. 
    the path like this: `hbase-1.0.1.1/hbase-thrift/src/main/resources/org/apache/hadoop/hbase/thrift`
2. the code in hbase direction generate by thrift
    like this: `thrift --gen py Hbase.thrift`
    
so if you use other hbase version, use up cmd to generate this code we need use.

when exec scanner, we can  get some result like this:


``` python
TRowResult(

    sortedColumns=None,

    columns={
        'default:yang': TCell(timestamp=1452847138947, value='zhijia'),
        'default:hi': TCell(timestamp=1452847138947, value='hello world')
        },

    row='row-key'

    )
```

so, i need translate it to a simple form
