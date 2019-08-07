package etg.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;

public class App {

    private static Admin admin;

    private static final String _TABLE_NAME = "user_table";
    private static final String _TABLE_NAME_COL1 = "information";
    private static final String _TABLE_NAME_COL2 = "contact";
    private static final String _TABLE_NAME_APPEND_COL = "appendfamily";

    public static void main(String[] args) throws IOException {
        System.out.println("--------------------建一个表" + _TABLE_NAME + "，里面包括两个列[" + _TABLE_NAME_COL1 + "," + _TABLE_NAME_COL2 + "]--------------------");
        createTable(_TABLE_NAME, new String[]{_TABLE_NAME_COL1, _TABLE_NAME_COL2});

        System.out.println("--------------------插入两条数据--------------------");
        User user1 = new User("001", "xiaoming", "123456", "20", "13355550021", "123456@qq.com");
        insertData(_TABLE_NAME, user1);
        User user2 = new User("002", "xiaohong", "123123", "18", "13355550022", "654321@qq.com");
        insertData(_TABLE_NAME, user2);
        List<User> list = getAllData(_TABLE_NAME);
        System.out.println("--------------------插入两条数据后--------------------");
        for (User user : list) {
            System.out.println(user.toString());
        }
        System.out.println("--------------------获取原始数据-----------------------");
        getNoDealData(_TABLE_NAME);
        System.out.println("--------------------根据rowKey查询--------------------");
        User user3 = getDataByRowKey(_TABLE_NAME, "user-001");
        System.out.println(user3.toString());
        System.out.println("--------------------获取指定单条数据-------------------");
        String user_phone = getCellData(_TABLE_NAME, "user-001", _TABLE_NAME_COL2, "phone");
        System.out.println(user_phone);
        System.out.println("--------------------再插一条数据-----------------------");
        User user4 = new User("test-003", "xiaoguang", "789012", "22", "12312132214", "856832@csdn.com");
        insertData(_TABLE_NAME, user4);
        List<User> list2 = getAllData(_TABLE_NAME);
        System.out.println("--------------------插入测试数据后--------------------");
        for (User user5 : list2) System.out.println(user5.toString());
        System.out.println("--------------------删除测试数据--------------------");
        deleteByRowKey(_TABLE_NAME, "user-test-003");
        List<User> list3 = getAllData(_TABLE_NAME);
        System.out.println("--------------------删除测试数据后--------------------");
        for (User user6 : list3) System.out.println(user6.toString());

        apendFamily(_TABLE_NAME, _TABLE_NAME_APPEND_COL);
        User user5 = new User("test-append", "append", "append", "22", "12312132214", "856832@csdn.com");
        insertData_new(_TABLE_NAME,user5);
        getNoDealData(_TABLE_NAME);
        System.out.println("--------------------删除刚添加的Family后--------------------");
        deleteFamily(_TABLE_NAME,_TABLE_NAME_APPEND_COL);
        getNoDealData(_TABLE_NAME);

        System.out.println("--------------------删除表--------------------");
        deleteTable(_TABLE_NAME);
    }

    private static void deleteByRowKey(String tableName, String rowKey) throws IOException {
        Table table = initHbase().getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }

    private static String getCellData(String tableName, String rowKey, String family, String col) throws IOException {
        Table table = initHbase().getTable(TableName.valueOf(tableName));
        String result = null;
        Get get = new Get(rowKey.getBytes());
        if (!get.isCheckExistenceOnly()) {
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(col));
            Result res = table.get(get);
            byte[] resByte = res.getValue(Bytes.toBytes(family), Bytes.toBytes(col));
            return result = Bytes.toString(resByte);
        } else {
            return result = "查询结果不存在";
        }
    }

    private static User getDataByRowKey(String tableName, String rowKey) throws IOException {
        Table table = initHbase().getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        User user = new User();
        user.setId(rowKey);
        if (!get.isCheckExistenceOnly()) {
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                if (colName.equals("username")) {
                    user.setUsername(value);
                } else if (colName.equals("age")) {
                    user.setAge(value);
                } else if (colName.equals("phone")) {
                    user.setPhone(value);
                } else if (colName.equals("email")) {
                    user.setEmail(value);
                }
            }
        }
        return user;
    }

    private static void getNoDealData(String tableName) throws IOException {
        Table table = initHbase().getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            System.out.println("scan: " + result);
        }
    }

    private static List<User> getAllData(String tableName) throws IOException {
        Table table = null;
        List<User> list = new ArrayList<User>();
        table = initHbase().getTable(TableName.valueOf(tableName));
        ResultScanner results = table.getScanner(new Scan());
        User user = null;
        for (Result result : results) {
            String id = new String(result.getRow());
            System.out.println("用户名：" + new String(result.getRow()));
            user = new User();
            for (Cell cell : result.rawCells()) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                user.setId(row);
                if (colName.equals("username")) {
                    user.setUsername(value);
                } else if (colName.equals("age")) {
                    user.setAge(value);
                } else if (colName.equals("phone")) {
                    user.setPhone(value);
                } else if (colName.equals("email")) {
                    user.setEmail(value);
                }
            }
            list.add(user);
        }
        return list;
    }

    private static void insertData_new(String tableName1, User user) throws IOException {
        TableName tableName = TableName.valueOf(tableName1);
        Put put = new Put(("user-" + user.getId()).getBytes());
        put.addColumn(_TABLE_NAME_COL1.getBytes(), "username".getBytes(), user.getUsername().getBytes());
        put.addColumn(_TABLE_NAME_COL1.getBytes(), "age".getBytes(), user.getAge().getBytes());
//        put.addColumn(_TABLE_NAME_COL1.getBytes(),"")
        put.addColumn(_TABLE_NAME_COL2.getBytes(), "phone".getBytes(), user.getPhone().getBytes());
        put.addColumn(_TABLE_NAME_COL2.getBytes(), "email".getBytes(), user.getEmail().getBytes());
        put.addColumn(_TABLE_NAME_APPEND_COL.getBytes(), "password".getBytes(), user.getPassword().getBytes());
        Table table = initHbase().getTable(tableName);
        table.put(put);
    }

    private static void insertData(String tableName1, User user) throws IOException {
        TableName tableName = TableName.valueOf(tableName1);
        Put put = new Put(("user-" + user.getId()).getBytes());
        put.addColumn(_TABLE_NAME_COL1.getBytes(), "username".getBytes(), user.getUsername().getBytes());
        put.addColumn(_TABLE_NAME_COL1.getBytes(), "age".getBytes(), user.getAge().getBytes());
//        put.addColumn(_TABLE_NAME_COL1.getBytes(),"")
        put.addColumn(_TABLE_NAME_COL2.getBytes(), "phone".getBytes(), user.getPhone().getBytes());
        put.addColumn(_TABLE_NAME_COL2.getBytes(), "email".getBytes(), user.getEmail().getBytes());
        Table table = initHbase().getTable(tableName);
        table.put(put);
    }

    public static void deleteFamily(String tableName1,String col) throws IOException {
        admin=initHbase().getAdmin();
        TableName tableName = TableName.valueOf(tableName1);
        if(admin.tableExists(tableName)){
            admin.deleteColumn(tableName,col.getBytes());
        }
    }

    public static void apendFamily(String tableName1, String col) throws IOException {
        admin = initHbase().getAdmin();
        TableName tableName = TableName.valueOf(tableName1);
        if (admin.tableExists(tableName)) {
            HTableDescriptor hTableDescriptor = admin.getTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
            hTableDescriptor.addFamily(hColumnDescriptor);
            admin.modifyTable(tableName, hTableDescriptor);
//            admin.modifyColumn(tableName,hColumnDescriptor);
        }
    }

    public static void createTable(String tableName1, String[] cols) throws IOException {
        TableName tableName = TableName.valueOf(tableName1);
        admin = initHbase().getAdmin();
        if (admin.tableExists(tableName)) {
            System.out.println("表已存在");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
    }

    public static Connection initHbase() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "server1,server2,server3");
        configuration.set("hbase.master", "server1:60000");
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    public static void deleteTable(String tableName1) throws IOException {
        TableName tableName = TableName.valueOf(tableName1);
        admin = initHbase().getAdmin();
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

}
