package top.wangbd.mydb.server.dm;

import top.wangbd.mydb.server.dm.Logger.Logger;
import top.wangbd.mydb.server.dm.dataItem.DataItem;
import top.wangbd.mydb.server.dm.page.PageOne;
import top.wangbd.mydb.server.dm.pageCache.PageCache;
import top.wangbd.mydb.server.tm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    /** 创建DataManager实例, 并构建.db和.log文件，初始化db文件的第一页数据 */
    public static DataManager create(String path, long mem, TransactionManager tm) {
        // 创建页面缓存和日志管理器
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        // 创建DataManagerImpl实例
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);

        // 初始化第一页
        dm.initPageOne();
        return dm;
    }

    /** 创建DataManager实例，加载.db和.log文件，校验第一页数据完整性 */
    public static DataManager open(String path, long mem, TransactionManager tm) {
        // 打开页面缓存和日志管理器
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);

        // 创建DataManagerImpl实例
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);

        // 加载并校验第一页数据完整性，如校验失败则进行恢复
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }

        // 填充页面索引
        dm.fillPageIndex();

        // 刷新第一页的校验码
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
