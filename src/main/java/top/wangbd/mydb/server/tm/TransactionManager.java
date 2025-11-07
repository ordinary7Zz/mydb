package top.wangbd.mydb.server.tm;

/**
 * 事务管理器接口
 * 负责管理数据库事务的生命周期，包括开始、提交、回滚以及状态查询
 */
public interface TransactionManager {
    /*** 开始一个新事务*/
    long begin();
    /*** 提交指定的事务*/
    void commit(long xid);
    /*** 回滚指定的事务*/
    void abort(long xid);
    /*** 检查事务是否处于活动状态*/
    boolean isActive(long xid);
    /*** 检查事务是否已提交*/
    boolean isCommitted(long xid);
    /*** 检查事务是否已回滚*/
    boolean isAborted(long xid);
    /*** 关闭事务管理器，释放相关资源*/
    void close();
}
