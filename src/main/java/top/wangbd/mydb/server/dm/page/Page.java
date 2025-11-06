package top.wangbd.mydb.server.dm.page;

/**
 * 页面接口
 */
public interface Page {
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}

