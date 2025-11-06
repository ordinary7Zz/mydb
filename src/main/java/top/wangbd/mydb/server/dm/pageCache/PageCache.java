package top.wangbd.mydb.server.dm.pageCache;

import top.wangbd.mydb.server.dm.page.Page;

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
}
