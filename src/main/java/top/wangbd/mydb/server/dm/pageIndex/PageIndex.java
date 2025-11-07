package top.wangbd.mydb.server.dm.pageIndex;

import top.wangbd.mydb.server.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<List<PageInfo>> lists;

    public PageIndex() {
        lock = new ReentrantLock();
        lists = new ArrayList<>(INTERVALS_NO + 1);
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists.add(new ArrayList<>());
        }
    }

    /*** 根据需要的空间大小, 选择合适的页返回*/
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            // 需要向上取整, 确保找到的页有足够的空间 比如如果需要1.5个THRESHOLD的空间, 那么就需要找至少2个THRESHOLD空间的页
            int number = (spaceSize + THRESHOLD - 1) / THRESHOLD;
            if (number > INTERVALS_NO) number = INTERVALS_NO;

            while(number <= INTERVALS_NO) {
                if(lists.get(number).size() == 0) {
                    number ++;
                    continue;
                }
                return lists.get(number).remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /*** 将有空闲空间的页加入到页面索引*/
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists.get(number).add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }
}
