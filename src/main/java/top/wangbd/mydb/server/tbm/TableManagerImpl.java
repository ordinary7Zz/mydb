package top.wangbd.mydb.server.tbm;

import top.wangbd.mydb.common.Error;
import top.wangbd.mydb.server.dm.DataManager;
import top.wangbd.mydb.server.parser.statement.*;
import top.wangbd.mydb.server.utils.Parser;
import top.wangbd.mydb.server.vm.VersionManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TableManagerImpl implements TableManager {
    VersionManager vm;
    DataManager dm;
    private Booter booter;
    private Map<String, Table> tableCache; // 记录所有的Table对象
    private Map<Long, List<Table>> xidTableCache; // 记录每个事务所涉及的Table对象
    private Lock lock;

    TableManagerImpl(VersionManager vm, DataManager dm, Booter booter) {
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.xidTableCache = new HashMap<>();
        lock = new ReentrantLock();
        loadTables();
    }


    /** 开启一个新事务 */
    @Override
    public BeginRes begin(Begin begin) {
        BeginRes res = new BeginRes();
        int level = begin.isRepeatableRead?1:0;
        res.xid = vm.begin(level);
        res.result = "begin".getBytes();
        return res;
    }

    /** 提交一个事务 */
    @Override
    public byte[] commit(long xid) throws Exception {
        vm.commit(xid);
        return "commit".getBytes();
    }

    /** 退回一个事务 */
    @Override
    public byte[] abort(long xid) {
        vm.abort(xid);
        return "abort".getBytes();
    }

    /** 显示所有表信息 */
    @Override
    public byte[] show(long xid) {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (Table tb : tableCache.values()) {
                sb.append(tb.toString()).append("\n");
            }
            // TODO: 这样show tables 可能会导致重复的表？
//            List<Table> t = xidTableCache.get(xid);
//            if(t == null) {
//                return "\n".getBytes();
//            }
//            for (Table tb : t) {
//                sb.append(tb.toString()).append("\n");
//            }
            return sb.toString().getBytes();
        } finally {
            lock.unlock();
        }
    }

    /** 创建一张新表 */
    @Override
    public byte[] create(long xid, Create create) throws Exception {
        lock.lock();
        try {
            if(tableCache.containsKey(create.tableName)) {
                throw Error.DuplicatedTableException;
            }
            // 创建表实例对象，并持久化
            Table table = Table.createTable(this, firstTableUid(), xid, create);
            // 更新第一个表的uid
            updateFirstTableUid(table.uid);
            // 将新表加入缓存
            tableCache.put(create.tableName, table);
            if(!xidTableCache.containsKey(xid)) {
                xidTableCache.put(xid, new ArrayList<>());
            }
            xidTableCache.get(xid).add(table);
            return ("create " + create.tableName).getBytes();
        } finally {
            lock.unlock();
        }
    }

    /** 插入数据，调用Table的insert方法 */
    @Override
    public byte[] insert(long xid, Insert insert) throws Exception {
        lock.lock();
        Table table = tableCache.get(insert.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        table.insert(xid, insert);
        return "insert".getBytes();
    }

    /** 读取数据，调用Table的read方法 */
    @Override
    public byte[] read(long xid, Select read) throws Exception {
        lock.lock();
        Table table = tableCache.get(read.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        return table.read(xid, read).getBytes();
    }

    /** 更新数据，调用Table的update方法 */
    @Override
    public byte[] update(long xid, Update update) throws Exception {
        lock.lock();
        Table table = tableCache.get(update.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.update(xid, update);
        return ("update " + count).getBytes();
    }

    /** 删除数据，调用Table的delete方法 */
    @Override
    public byte[] delete(long xid, Delete delete) throws Exception {
        lock.lock();
        Table table = tableCache.get(delete.tableName);
        lock.unlock();
        if(table == null) {
            throw Error.TableNotFoundException;
        }
        int count = table.delete(xid, delete);
        return ("delete " + count).getBytes();
    }

    /** 加载所有的表信息到tableCache中 */
    private void loadTables() {
        // 获取第一个表的uid
        long uid = firstTableUid();
        // 遍历所有表，加载到tableCache中
        while(uid != 0) {
            Table tb = Table.loadTable(this, uid);
            uid = tb.nextUid;
            tableCache.put(tb.name, tb);
        }
    }

    /** 获取第一个表的uid */
    private long firstTableUid() {
        byte[] raw = booter.load();
        return Parser.parseLong(raw);
    }

    /** 更新第一个表的uid */
    private void updateFirstTableUid(long uid) {
        byte[] raw = Parser.long2Byte(uid);
        booter.update(raw);
    }
}
