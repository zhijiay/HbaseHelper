from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import ColumnDescriptor, Mutation, TScan


class HbaseHelper:
    '''
    a simple hbase client by yangzhijia 2016.01.15
    '''

    def __init__(self, ip, port):
        '''
        init the helper with thrift server ip address and port
        :param ip: ip address
        :param port: port
        :return: no return
        '''

        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(ip, port))
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(self.protocol)
        self.transport.open()
        self.tables = self.client.getTableNames()

    def __del__(self):
        self.transport.close()

    def createTable(self, tablename):
        '''
        create table in hbase
        :param tablename: the table name
        :return: no return
        '''
        name = ColumnDescriptor(name='default')
        self.client.createTable(tablename, [name])

    def deleteTable(self, tablename):
        '''
        delete one tabe in hbase ( drop ont table need disable it first )
        :param tablename:
        :return:
        '''
        if tablename in self.tables:
            self.client.disable(tablename)
            self.client.deleteTable(tablename)
            self.tables = self.client.getTableNames()

    def put(self, tablename, rowkey, data):
        '''
        put data to hbase table
        :param tablename: the table name
        :param rowkey: the row key
        :param data: a dic for data. it is like this { column1:value1, column2:value2, ... }
        :return: no return
        '''
        if tablename not in self.tables:
            self.createTable(tablename)
            self.tables = self.client.getTableNames()

        mutations = []
        for column in data:
            mutations.append(Mutation(column='default:' + column, value=data[column]))

        self.client.mutateRow(tablename, rowkey, mutations, None)

    def get(self, tablename, rowkey):
        pass

    def result2dic(self, result):
        '''
        the return of 'scannerGetList' is a list of TRowResult.
        like this:

            TRowResult(

                sortedColumns=None,

                columns={
                    'default:yang': TCell(timestamp=1452847138947, value='zhijia'),
                    'default:hi': TCell(timestamp=1452847138947, value='hello world')
                    },

                row='row-key'

                )

        so, i need turn it to a easy form( as a dic )

        :param result: TRowResult
        :return: row key and dic from TrowResult
        '''
        rowkey = result.row
        data = {}
        for column in result.columns:
            data[column] = result.columns[column].value

        return rowkey, data

    def scanner(self, tablename, callback, startRow=None, stopRow=None):
        '''
        a scanner to scan hbase table from start row to end row, and call callback function
        callback function like this:

            def show(rowkey, data):
                print 'rowkey:', rowkey
                for key in data:
                    print '    ', key, ':', data[key]

            the rowkey is the row key, the data is the return of result2dic()

        :param tablename: the table name you need to scan
        :param callback: the callback function
        :param startRow: the begin row key, if you do not need it, keep it as None
        :param endRow: the last row key, if you do not need it, keep it as None
        :return: no return
        '''
        scan = TScan(startRow=startRow, stopRow=stopRow)
        sid = self.client.scannerOpenWithScan(tablename, scan, None)

        while True:
            results = self.client.scannerGetList(sid, 100)  # it is faster than scannerGet
            if len(results) == 0:
                break

            for result in results:
                rowkey, data = self.result2dic(result)
                callback(rowkey, data)

        self.client.scannerClose(sid)


def test():
    hbase = HbaseHelper('192.168.1.56', 9090)
    hbase.put('mytest', 'row-key', {'hi': 'hello world'})
    hbase.put('mytest', 'row-key2', {'hi': 'hello world', 'yang': 'zhijia'})

    def show(rowkey, data):
        print 'rowkey:', rowkey
        for key in data:
            print '    ', key, ':', data[key]

    hbase.scanner('mytest', show)


if __name__ == '__main__':
    test()

