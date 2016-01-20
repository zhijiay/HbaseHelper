'''
    copy and change the data to another table with thrift1
'''

from thrift1 import HbaseHelper


def translate(tablename):

    opt = HbaseHelper.HbaseHelper('192.168.1.56', 9090)  # get a hbase helper for put
    scan = HbaseHelper.HbaseHelper('192.168.1.56', 9090)  # get a hbase helper for scan

    def translater(rowkey, data):
        new_data = {}
        for key in data:
            if key == 'hello':
                new_data[key] = 'world'
            else:
                new_data[key] = data[key]

        opt.put(tablename+'-hello', rowkey, new_data)
        opt.delete(tablename, rowkey)
        print tablename, rowkey

    scan.scanner(tablename, translater, None, None)


if __name__ == '__main__':
    translate('mytest')

