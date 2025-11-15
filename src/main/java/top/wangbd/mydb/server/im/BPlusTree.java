package top.wangbd.mydb.server.im;

import top.wangbd.mydb.server.common.SubArray;
import top.wangbd.mydb.server.dm.DataManager;
import top.wangbd.mydb.server.dm.dataItem.DataItem;
import top.wangbd.mydb.server.tm.TransactionManagerImpl;
import top.wangbd.mydb.server.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BPlusTree {
    DataManager dm;
    long bootUid;
    DataItem bootDataItem; // 存储B+树根节点UID的dataItem
    Lock bootLock;

    /** 创建一个新的B+树，返回bootUid */
    public static long create(DataManager dm) throws Exception {
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        return dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
    }

    /** 通过读取bootUid的数据，加载一棵已有的B+树实例 */
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception {
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    /** 根据key精准查询B+树中对应的记录UID列表
     *  返回结果是列表，是因为key不一定唯一
     * */
    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    /** 在B+树中查找[leftKey, rightKey]范围内的所有记录UID */
    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while(true) {
            Node leaf = Node.loadNode(this, leafUid);
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    class InsertRes {
        long newNode, newKey;
    }

    /** 向B+树中插入(uid, key)键值对
     *  若返回结果的newNode不为0，表示根节点发生了分裂，需要创建新的根节点
     * */
    public void insert(long key, long uid) throws Exception {
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        assert res != null;
        if(res.newNode != 0) {
            updateRootUid(rootUid, res.newNode, res.newKey);
        }
    }

    public void close() {
        bootDataItem.release();
    }

    /** 获取B+树的根节点UID
     * bootUid 位置存储着根节点的UID（bootDataItem）
     * */
    private long rootUid() {
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }

    /** 更新B+树的根节点UID
     *  当根节点分裂时，创建新的根节点，并更新bootUid位置存储的根节点UID数据
     * */
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    /** 在nodeUid节点中，查找key应该进入的下一个子节点UID
     *  nodeUid一定是一个非叶子节点
     * */
    private long searchNext(long nodeUid, long key) throws Exception {
        // while循环不断遍历当前nodeUid及其兄弟节点，直到找到合适的子节点UID返回
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.SearchNextRes res = node.searchNext(key);
            node.release();
            if(res.uid != 0) return res.uid;
            nodeUid = res.siblingUid;
        }
    }


    /** 从nodeUid节点开始，递归查找key所在的叶子节点UID */
    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if(isLeaf) {
            return nodeUid;
        } else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }


    /** 递归地将(uid, key)插入到nodeUid节点所在的子树中
     *  返回值InsertRes表示插入后是否产生了新的兄弟节点需要上升到父节点
     * - newNode = 0, newKey = 0 ：插入成功， 未发生节点分裂 ，树结构无需调整
     * - newNode ≠ 0, newKey ≠ 0 ：插入成功， 发生了节点分裂 ，需要上层处理新节点
     * */
    private InsertRes insert(long nodeUid, long uid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        InsertRes res = null;
        if(isLeaf) {
            res = insertAndSplit(nodeUid, uid, key);
        } else {
            // 如果是非叶子节点，则先找到下一个子节点，递归插入
            long next = searchNext(nodeUid, key);
            InsertRes ir = insert(next, uid, key);
            if(ir.newNode != 0) {
                // 下层节点发生了分裂，需要将新节点信息插入当前节点
                res = insertAndSplit(nodeUid, ir.newNode, ir.newKey);
            } else {
                res = new InsertRes();
            }
        }
        return res;
    }

    /** 在nodeUid节点中插入(uid, key)
     *  如果节点分裂，则返回新的兄弟节点信息，否则返回空信息
     *  - newNode = 0, newKey = 0: 插入成功且无分裂，无需上层处理
     *  - newNode ≠ 0, newKey ≠ 0: 插入成功且发生分裂，newNode为新节点UID，newKey为新节点最小键值
     * */
    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {
        while(true) {
            // while循环是为了处理当前节点插入失败，需要在兄弟节点继续插入的情况
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if(iasr.siblingUid != 0) {
                nodeUid = iasr.siblingUid;
            } else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }


}
