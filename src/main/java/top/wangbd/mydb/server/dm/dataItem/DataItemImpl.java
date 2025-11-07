package top.wangbd.mydb.server.dm.dataItem;

import top.wangbd.mydb.server.common.SubArray;
import top.wangbd.mydb.server.dm.DataManagerImpl;
import top.wangbd.mydb.server.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataItemImpl implements DataItem{

    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    private SubArray raw;      // dataitem的全部数据，包括有效位和长度
    private byte[] oldRaw;     // 用于before和unBefore操作，保存修改前的数据
    private Lock rLock;        // 读锁
    private Lock wLock;        // 写锁
    private DataManagerImpl dm;// 数据管理器
    private long uid;          // dataitem的唯一标识
    private Page pg;           // dataitem所在的页面

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    /*** 判断dataitem是否有效 */
    public boolean isValid() {
        return raw.raw[raw.start+OF_VALID] == (byte)0;
    }

    @Override
    public SubArray data() {
        // 返回DataItem的data部分
        return new SubArray(raw.raw, raw.start+OF_DATA, raw.end);
    }

    @Override
    public void before() {
        // 获取写锁，标记页面为脏页，保存修改前的数据
        wLock.lock();
        pg.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    @Override
    public void unBefore() {
        // 恢复修改前的数据，释放写锁
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    @Override
    public void after(long xid) {
        // 将dataitem的修改记录到日志中，释放写锁
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    @Override
    public void release() {
        // 释放dataitem
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
