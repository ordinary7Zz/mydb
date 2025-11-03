package top.wangbd.mydb.server.tm;

/**
 * 事务管理器接口
 * 负责管理数据库事务的生命周期，包括开始、提交、回滚以及状态查询
 */
public interface TransactionManager {
    /**
     * 开始一个新事务
     *
     * @return 新事务的ID（xid）
     */
    long begin();

    /**
     * 提交指定的事务
     *
     * @param xid 事务ID
     */
    void commit(long xid);

    /**
     * 回滚指定的事务
     *
     * @param xid 事务ID
     */
    void abort(long xid);

    /**
     * 检查事务是否处于活动状态
     *
     * @param xid 事务ID
     * @return 如果事务处于活动状态返回true，否则返回false
     */
    boolean isActive(long xid);

    /**
     * 检查事务是否已提交
     *
     * @param xid 事务ID
     * @return 如果事务已提交返回true，否则返回false
     */
    boolean isCommitted(long xid);

    /**
     * 检查事务是否已回滚
     *
     * @param xid 事务ID
     * @return 如果事务已回滚返回true，否则返回false
     */
    boolean isAborted(long xid);

    /**
     * 关闭事务管理器，释放相关资源
     */
    void close();
}
