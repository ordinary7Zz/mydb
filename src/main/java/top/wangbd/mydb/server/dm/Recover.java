package top.wangbd.mydb.server.dm;

import com.google.common.primitives.Bytes;
import top.wangbd.mydb.server.common.SubArray;
import top.wangbd.mydb.server.dm.Logger.Logger;
import top.wangbd.mydb.server.dm.dataItem.DataItem;
import top.wangbd.mydb.server.dm.page.Page;
import top.wangbd.mydb.server.dm.page.PageX;
import top.wangbd.mydb.server.dm.pageCache.PageCache;
import top.wangbd.mydb.server.tm.TransactionManager;
import top.wangbd.mydb.server.utils.Panic;
import top.wangbd.mydb.server.utils.Parser;

import java.util.*;

public class Recover {
    // 规定两种日志格式的字节标识
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;
    // updateLog: [LogType] [XID] [UID] [OldRaw] [NewRaw]
    // insertLog: [LogType] [XID] [Pgno] [Offset] [Raw]

    private static final int REDO = 0;
    private static final int UNDO = 1;


    /*** 日志字段的偏移量*/
    private static final int OF_TYPE = 0; //日志类型 1字节
    private static final int OF_XID = OF_TYPE+1; // 事务ID 8字节
    /*** 更新日志字段的偏移量*/
    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_UPDATE_UID = OF_XID+8; // 数据项唯一标识 8字节
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID+8; // 旧数据起始位置
    /*** 插入日志字段的偏移量*/
    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID+8;  // 页号 4字节
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO+4; // 偏移量 2字节
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET+2; // 数据起始位置

    // 插入日志的信息结构
    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    // 更新日志的信息结构
    // uid 是记录（DataItem）的全局标识符，把页号和页内偏移合并成一个 long 存储。
    // [高 32 位: pgno][中 16 位: 未用][低 16 位: offset]（即 uid = (pgno << 32) | offset）。
    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    /**
     * 数据库恢复入口
     */
    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        lg.rewind();

        // 找到日志中涉及的最大页号
        int maxPgno = 0;
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            int pgno;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if(pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        if(maxPgno == 0) {
            maxPgno = 1;
        }
        // 截断或扩展页文件到 maxPgno 页
        pc.truncateByBgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTransactions(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        undoTransactions(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");

    }

    /*** 生成一条插入日志*/
    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    /*** 生成一条更新日志*/
    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    /*** 重做日志*/
    public static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        // 日志文件指针回到文件开头
        lg.rewind();

        while (true) {
            // 读取下一条日志
            byte[] log_data = lg.next();
            if(log_data == null) break;
            if (isInsertLog(log_data)) {
                // 解析插入日志
                InsertLogInfo li = parseInsertLog(log_data);
                long xid = li.xid;
                if(!tm.isActive(xid)) {
                    doInsertLog(pc, li, REDO);
                }
            } else {
                // 解析更新日志
                UpdateLogInfo xi = parseUpdateLog(log_data);
                long xid = xi.xid;
                if(!tm.isActive(xid)) {
                    doUpdateLog(pc, xi, REDO);
                }
            }
        }
    }

    /**
     * 撤销未提交的事务
     */
    public static void undoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        // 记录所有未提交的事务的日志 [事务ID 日志列表]
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        // 日志文件指针回到文件开头
        lg.rewind();

        // 遍历日志文件，找到所有未提交事务的日志并记录下来
        while (true) {
            // 读取下一条日志
            byte[] log_data = lg.next();
            if(log_data == null) break;
            if (isInsertLog(log_data)) {
                // 解析插入日志
                InsertLogInfo li = parseInsertLog(log_data);
                long xid = li.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log_data);
                }
            } else {
                // 解析更新日志
                UpdateLogInfo xi = parseUpdateLog(log_data);
                long xid = xi.xid;
                if (tm.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log_data);
                }
            }
        }

        // 对每个未提交事务的日志进行撤销操作
        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> value = entry.getValue();

            // 倒序对该事务的日志列表进行撤销
            for (int i = value.size() - 1; i >= 0; i--) {
                byte[] log_data = value.get(i);
                if (isInsertLog(log_data)) {
                    InsertLogInfo li = parseInsertLog(log_data);
                    doInsertLog(pc, li, UNDO);
                } else {
                    UpdateLogInfo xi = parseUpdateLog(log_data);
                    doUpdateLog(pc, xi, UNDO);
                }
            }

            // 将该事务的状态设置为已撤销
            tm.abort(entry.getKey());
        }
    }

    /**
     * 执行更新日志的重做或撤销操作
     */
    private static void doUpdateLog(PageCache pc, UpdateLogInfo xi, int flag) {
        int pgno = xi.pgno;
        short offset = xi.offset;
        byte[] raw;
        if (flag == REDO) {
            raw = xi.newRaw;
        } else {
            raw = xi.oldRaw;
        }

        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

    /**
     * 执行插入日志的重做或撤销操作
     */
    private static void doInsertLog(PageCache pc, InsertLogInfo li, int flag) {
        Page pg = null;
        try {
            pg = pc.getPage(li.pgno);
        } catch(Exception e) {
            Panic.panic(e);
        }
        try {
            if (flag == UNDO){
                // 把数据的有效位置为无效，因此下面的插入不在else内，而是必经路径
                DataItem.setDataItemRawInvalid(li.raw);
            }
            // 向该页指定偏移位置插入数据
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            pg.release();
        }
    }


    /*** 判断日志是否为插入日志或更新日志*/
    private static boolean isInsertLog(byte[] log) {
        // 日志的第一个字节表示日志类型，只有插入和更新两种类型
        return log[0] == LOG_TYPE_INSERT;
    }

    /*** 解析插入日志*/
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    /*** 解析更新日志*/
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));

        // 解析 uid，拆分为 pgno 和 offset
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32; // >>= 保留符号位（算术右移），>>>= 用 0 填充（逻辑右移）
        li.pgno = (int)(uid & ((1L << 32) - 1));

        // 解析 oldRaw 和 newRaw
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return li;
    }


    /**
     * 其实也可以把redo和undo合并成一个方法
     */
    private static void redoAndUndoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> activeLogs = new HashMap<>();
        lg.rewind();
        // 单次前向遍历：对非 active 的 redo，对 active 的收集用于之后 undo
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if (!tm.isActive(xid)) {
                    doInsertLog(pc, li, REDO);
                } else {
                    activeLogs.computeIfAbsent(xid, k -> new ArrayList<>()).add(log);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if (!tm.isActive(xid)) {
                    doUpdateLog(pc, xi, REDO);
                } else {
                    activeLogs.computeIfAbsent(xid, k -> new ArrayList<>()).add(log);
                }
            }
        }

        // 对每个 active 事务的记录倒序 undo 并 abort
        for (Map.Entry<Long, List<byte[]>> entry : activeLogs.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)) {
                    InsertLogInfo li = parseInsertLog(log);
                    doInsertLog(pc, li, UNDO);
                } else {
                    UpdateLogInfo ui = parseUpdateLog(log);
                    doUpdateLog(pc, ui, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

}
