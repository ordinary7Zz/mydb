package top.wangbd.mydb.server.vm;

import top.wangbd.mydb.server.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

public class Transaction {
    public long xid; // 事务ID
    public int level; // 隔离级别，0表示读已提交，1表示可重复读
    public Map<Long, Boolean> snapshot; // 事务快照，用于可重复读隔离级别
    public Exception err; // 事务错误信息
    public boolean autoAborted; // 是否自动中止

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        // 仅在可重复读隔离级别下创建快照
        if(level != 0) {
            t.snapshot = new HashMap<>();
            // 复制当前活跃事务的ID到快照中
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
