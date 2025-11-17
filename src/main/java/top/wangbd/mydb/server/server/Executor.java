package top.wangbd.mydb.server.server;

import top.wangbd.mydb.common.Error;
import top.wangbd.mydb.server.parser.TokenParser;
import top.wangbd.mydb.server.parser.statement.*;
import top.wangbd.mydb.server.tbm.BeginRes;
import top.wangbd.mydb.server.tbm.TableManager;
import top.wangbd.mydb.server.utils.Parser;

public class Executor {
    private long xid; // 当前事务id，0表示SUPER_XID
    TableManager tbm; // 表管理器

    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;
    }

    public void close() {
        if(xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
            tbm.abort(xid);
        }
    }

    /** 执行一条sql语句，返回执行结果
     *  此方法仅处理事务相关语句，其他语句交给execute2处理
     * */
    public byte[] execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        Object stat = TokenParser.Parse(sql);
        // 开启一个事务
        if(Begin.class.isInstance(stat)) {
            if(xid != 0) {
                throw Error.NestedTransactionException;
            }
            BeginRes r = tbm.begin((Begin)stat);
            xid = r.xid;
            return r.result;
        } else if(Commit.class.isInstance(stat)) {
            // 提交当前事务
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;
        } else if(Abort.class.isInstance(stat)) {
            // 中止当前事务
            if(xid == 0) {
                throw Error.NoTransactionException;
            }
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        } else {
            return execute2(stat);
        }
    }

    /** 执行非事务相关语句 */
    private byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        if(xid == 0) {
            // 如果没有手动开启事务，则为该语句临时开启一个事务
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            xid = r.xid;
        }
        try {
            // 除了事务相关语句外的其他语句，调用TableManager处理
            byte[] res = null;
            if(Show.class.isInstance(stat)) {
                res = tbm.show(xid);
            } else if(Create.class.isInstance(stat)) {
                res = tbm.create(xid, (Create)stat);
            } else if(Select.class.isInstance(stat)) {
                res = tbm.read(xid, (Select)stat);
            } else if(Insert.class.isInstance(stat)) {
                res = tbm.insert(xid, (Insert)stat);
            } else if(Delete.class.isInstance(stat)) {
                res = tbm.delete(xid, (Delete)stat);
            } else if(Update.class.isInstance(stat)) {
                res = tbm.update(xid, (Update)stat);
            }
            return res;
        } catch(Exception e1) {
            e = e1;
            throw e;
        } finally {
            if(tmpTransaction) {
                if(e != null) {
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }
}
