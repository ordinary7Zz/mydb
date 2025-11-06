package top.wangbd.mydb.server.dm.pageCache;

import top.wangbd.mydb.common.Error;
import top.wangbd.mydb.server.common.AbstractCache;
import top.wangbd.mydb.server.dm.page.Page;
import top.wangbd.mydb.server.dm.page.PageImpl;
import top.wangbd.mydb.server.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static final int MEM_MIN_LIM = 10; // 最小内存限制，单位为页
    public static final String DB_SUFFIX = ".db"; // 数据库文件后缀

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNumbers;


    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
    }

    private static long pageOffset(int pgno) {
        return (pgno-1) * PAGE_SIZE;
    }

    /**
     * 将页面数据写回到文件中
     */
    private void flush(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);
        fileLock.lock();
        try {
            //用 wrap() 将页面数据包装成 ByteBuffer,然后通过 FileChannel.write() 写入文件
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }


    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno, initData, null);
        flush(pg);  // 新建的页面需要立刻写回
        return pgno;
    }

    /**
     * 获取指定页号的页面，读取到缓存中
     */
    @Override
    public Page getPage(int pgno) throws Exception {
        // 通过调用 AbstractCache 的 get 方法，从缓存或数据源（文件系统）获取页面
        return get((long)pgno);
    }

    /**
     * 关闭页面缓存并释放相关资源
     */
    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 释放页面
     */
    @Override
    public void release(Page page) {
        // 调用AbstractCache的 release 方法，传入页号作为键
        release((long)page.getPageNumber());
    }

    /**
     * 把数据库文件截断到只保留页号 (1..maxPgno) 对应的字节。
     */
    @Override
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        fileLock.lock();
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }

    /**
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = PageCacheImpl.pageOffset(pgno);
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno, buf.array(), this);
    }

    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }

    }
}
