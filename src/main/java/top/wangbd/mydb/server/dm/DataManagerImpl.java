package top.wangbd.mydb.server.dm;

import top.wangbd.mydb.common.Error;
import top.wangbd.mydb.server.common.AbstractCache;
import top.wangbd.mydb.server.dm.Logger.Logger;
import top.wangbd.mydb.server.dm.dataItem.DataItem;
import top.wangbd.mydb.server.dm.dataItem.DataItemImpl;
import top.wangbd.mydb.server.dm.page.Page;
import top.wangbd.mydb.server.dm.page.PageOne;
import top.wangbd.mydb.server.dm.page.PageX;
import top.wangbd.mydb.server.dm.pageCache.PageCache;
import top.wangbd.mydb.server.dm.pageIndex.PageIndex;
import top.wangbd.mydb.server.dm.pageIndex.PageInfo;
import top.wangbd.mydb.server.tm.TransactionManager;
import top.wangbd.mydb.server.utils.Panic;
import top.wangbd.mydb.server.utils.Types;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    /** 在创建文件时初始化PageOne，并赋给pageOne*/
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

    /** 在打开已有文件时时读入PageOne，并验证正确性 */
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /** 释放dataitem */
    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    /** 初始化pageIndex */
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        // 从第2页开始，第1页是pageOne
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }

    /** 缓存中不存在，从数据源中获取DataItem */
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        // 解析uid，获取对应的page和offset
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));

        // 获得DataItem所在的页面，如果页面不在缓存中则把页面加载到页面缓存中
        Page pg = pc.getPage(pgno);
        // 从页面和偏移量中解析出DataItem
        return DataItem.parseDataItem(pg, offset, this);
    }

    /** 把新的DataItem写入到数据源 */
    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    /** 为xid生成update日志*/
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    /** 插入操作：插入新的DataItem，并记录日志 */
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        // 把data包装成DataItem的原始格式
        byte[] raw = DataItem.wrapDataItemRaw(data);
        // 规定数据不能超过一页的最大可用空间
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        // 从pIndex中选择一个有足够空间的页面
        PageInfo pi = null;
        // 尝试5次寻找可用页面
        for(int i = 0; i < 5; i ++) {
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                // 没有找到合适的页面，创建一个新的页面
                // 虽然这里创建了一个新页面，但因为可能有并发插入，所以不能保证下次select就能选到这个页面，所以要尝试多次
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }

        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;

        try {
            // 获取选中的页面
            pg = pc.getPage(pi.pgno);

            // 生成插入日志并记录
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            // 执行插入操作
            short offset = PageX.insert(pg, raw);

            pg.release();

            // 返回新插入数据的uid
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                // 在插入过程中如果pg获取失败，则将原先的freeSpace值重新加入pIndex(其实没有太弄清楚这个条件什么时候触发)
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    /** 关闭DataManager，释放资源 */
    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }
}
