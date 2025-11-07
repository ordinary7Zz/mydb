package top.wangbd.mydb.server.dm.pageCache;

import top.wangbd.mydb.common.Error;
import top.wangbd.mydb.server.dm.page.Page;
import top.wangbd.mydb.server.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * 页面缓存接口
 */
public interface PageCache {
    public static final int PAGE_SIZE = 1 << 13; // 页面大小为8K
    /*** 创建一个新的数据页*/
    int newPage(byte[] initData);
    /*** 根据页号获取页面*/
    Page getPage(int pgno) throws Exception;
    /*** 关闭页面缓存*/
    void close();
    /*** 释放页面的引用*/
    void release(Page page);
    /*** 截断数据库文件到指定的最大页号
     * 删除页号大于 maxPgno 的所有页面，通常用于数据库恢复或回滚操作。*/
    void truncateByBgno(int maxPgno);
    /*** 获取当前数据库的页面总数
     * 返回数据库文件中已分配的页面数量（包括第一个特殊页PageOne）。*/
    int getPageNumber();
    /*** //将指定页面强制刷新到磁盘*/
    void flushPage(Page pg);

    /** 创建一个页面缓存实例，并创建数据库文件*/
    public static PageCacheImpl create(String path, long memory) {
        // 创建数据库文件，注意db文件是在构建PageCacheImpl的时候创建（实际相差不大，因为PageCacheImpl在DataManager中创建）
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }

        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }

    /** 创建一个页面缓存实例，并打开已有的数据库文件*/
    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }
}
