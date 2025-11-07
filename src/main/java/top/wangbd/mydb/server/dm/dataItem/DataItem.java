package top.wangbd.mydb.server.dm.dataItem;

import top.wangbd.mydb.server.common.SubArray;
import top.wangbd.mydb.server.dm.page.Page;

public interface DataItem {
    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();


}
