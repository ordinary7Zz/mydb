package top.wangbd.mydb.server.vm;

import top.wangbd.mydb.common.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID（一个事务同时只能等待一个uid）
    private Lock lock;


    /** 进行死锁检测使用到的数据结构 */
    private Map<Long, Integer> xidStamp;
    private int stamp;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /** xid请求uid的锁
     *  不需要等待则返回null，否则返回锁对象
     *  会造成死锁则抛出异常
     * */
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            // 查询xid是否已经持有uid
            if(isInList(x2u, xid, uid)) {
                return null;
            }
            // 查询uid是否被其他xid持有
            if(!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            // uid被其他xid持有，添加等待关系
            waitU.put(xid, uid);
            putIntoList(wait, uid, xid);

            // 检测是否有死锁，如果有则抛出异常
            if(hasDeadLock()) {
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }

            // 创建等待锁，并放入等待锁表
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;
        } finally {
            lock.unlock();
        }
    }

    /** 移除xid的所有锁和等待关系 */
    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> l = x2u.get(xid);
            if(l != null) {
                while(l.size() > 0) {
                    Long uid = l.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);

        } finally {
            lock.unlock();
        }
    }

    /** 从等待队列中选择一个xid来占用uid */
    private void selectNewXID(long uid) {
        // 先移除uid对应的xid
        u2x.remove(uid);
        // 获取uid对应的等待列表
        List<Long> l = wait.get(uid);
        if(l == null) return;
        assert l.size() > 0;

        while(l.size() > 0) {
            // 从等待列表的头部获取一个xid
            long xid = l.remove(0);
            // 检查xid是否还在等待锁表中，这里是因为remove中并没有从wait列表中移除（uid，xid），所以需要手动检查xid是否在等待uid
            if(!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.unlock();
                break;
            }
        }

        if(l.size() == 0) wait.remove(uid);
    }

    /** 检测当前是否存在死锁 */
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        // 遍历所有等待的xid，进行深度优先搜索
        for(long xid : x2u.keySet()) {
            // 如果能够从xidStamp中找到xid，且标记值大于0，说明已经访问过，跳过
            Integer s = xidStamp.get(xid);
            if(s != null && s > 0) {
                continue;
            }
            //
            stamp ++;
            // 从xid开始进行深度优先搜索
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    /** 深度优先搜索，检测是否存在环
     *  dfs是一个递归函数，任何能从xid访问到的节点都会被标记为相同的stamp
     *  如果在搜索过程中再次访问到已经被标记为当前stamp的节点，说明存在环，返回true
     * */
    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if(stp != null && stp == stamp) {
            return true;
        }
        if(stp != null && stp < stamp) {
            return false;
        }
        // 标记当前xid为当前stamp
        xidStamp.put(xid, stamp);

        // 获取xid正在等待的uid
        Long uid = waitU.get(xid);
        // 如果uid为null，说明xid没有在等待任何资源，直接返回false
        if(uid == null) return false;

        // 获取uid被哪个xid持有，递归进行深度优先搜索
        Long x = u2x.get(uid);
        assert x != null;
        return dfs(x);
    }

    /** 判断uid1是否在uid0对应的列表中 */
    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return false;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }

    /** 将uid1加入到uid0对应的列表的尾部 */
    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(uid1); // 原作者是加入到头部，但加入到尾部应该更符合公平等待队列的逻辑
    }

    /** 将uid1从uid0对应的列表中移除，如果uid0对应的列表没有数据，则移除uid0的列表 */
    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }


}