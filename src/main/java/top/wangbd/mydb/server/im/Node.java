package top.wangbd.mydb.server.im;

import top.wangbd.mydb.server.common.SubArray;
import top.wangbd.mydb.server.dm.dataItem.DataItem;
import top.wangbd.mydb.server.tm.TransactionManagerImpl;
import top.wangbd.mydb.server.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node结构如下：
 * [LeafFlag]1 [KeyNumber]2 [SiblingUid]8
 * [Son0]8 [Key0]8 [Son1][Key1]...[SonN][KeyN]
 *
 * 关于Son 和 Key值：
 * mydb B+树的查找规则：如果 key < Key[i]，则进入 Son[i]
 * 只有叶子节点保存的 key 才是真正匹配该 son 的 key 值
 * 内部节点：Key 是分界标记，不是精确对应
 */
public class Node {
    static final int IS_LEAF_OFFSET = 0; // 叶子节点标志位
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1; // 节点中Key的数量
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET+2;  // 兄弟节点UID
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET+8; // Node头部大小

    static final int BALANCE_NUMBER = 32; // 平衡因子
    // 额外的 2 对：预留空间，用于插入时的临时存储
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2); // Node总大小

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    public void release() {
        dataItem.release();
    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    /** 在当前节点中，根据Key查找子节点的UID
     *  此函数用于在非叶子节点的导航，因此使用的是开区间
     * */
    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);
            for(int i = 0; i < noKeys; i ++) {
                long ik = getRawKthKey(raw, i);
                if(key < ik) {
                    // 如果在当前的Node找到，uid设置为子节点uid，siblingUid设置为0
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            // 如果在当前的Node没有找到，uid设置为0，siblingUid设置为当前Node的siblingUid
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    /** 在叶子节点中，根据左右Key范围查找符合条件的UID列表
     *  此函数用于在叶子节点中按范围查找，因此使用的是闭区间
     * */
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            int noKeys = getRawNoKeys(raw);
            int kth = 0;

            // 先找到左边界
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik >= leftKey) {
                    break;
                }
                kth ++;
            }

            // 寻找右边界，把处在左右边界的uid加入到列表
            List<Long> uids = new ArrayList<>();
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth ++;
                } else {
                    break;
                }
            }
            long siblingUid = 0;
            // 如果 rightKey 大于等于该节点的最大的 key, 则还同时返回兄弟节点的 UID，方便继续搜索下一个节点。
            if(kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    /** 创建一个新的空根节点的raw数据，此时既是根节点也是叶子节点 */
    static byte[] newNilRootRaw()  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    /** 创建一个新的根节点的raw数据，包含两个子节点（刚分裂的根节点的度数一定为2）
     *  返回结果为新的根节点的UID
     * */
    static byte[] newRootRaw(long left, long right, long key)  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);

        // Long.MAX_VALUE不是一个uid的key值，而是一个哨兵值
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    /** 插入一个键值对到节点中，如果节点已满则进行分裂，返回分裂结果
     * - 普通插入 （不需要分裂）： newNode=0, newKey=0
     * - 节点分裂 ： newNode=新节点UID, newKey=分裂键值
     * */
    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before();
        try {
            // 向节点中插入新节点
            success = insert(uid, key);
            if(!success) {
                // 插入失败，说明节点已满，返回兄弟节点UID，等待上层继续插入
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            if(needSplit()) {
                try {
                    // 如果插入后需要分裂，则进行分裂操作，返回新节点UID和分裂Key
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch(Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if(err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }
    }

    private boolean needSplit() {
        return BALANCE_NUMBER*2 == getRawNoKeys(raw);
    }

    /** 向节点中插入一个键值对，如果节点已满则返回false */
    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        while(kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if(ik < key) {
                kth ++;
            } else {
                break;
            }
        }
        if(kth == noKeys && getRawSibling(raw) != 0) return false;

        // 如果插入的是叶子节点
        if(getRawIfLeaf(raw)) {
            shiftRawKth(raw, kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys+1);
        }
        // 如果插入的是内部节点。插入的是内部节点，说明kth的Son被分裂了，Son现在只保留了左半部分
        else {
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth+1);
            setRawKthKey(raw, kk, kth+1);
            setRawKthSon(raw, uid, kth+1);
            setRawNoKeys(raw, noKeys+1);
        }
        return true;
    }

    class SplitRes {
        long newSon, newKey;
    }

    /** 向右分裂一个节点，返回新节点的UID其中最小的Key */
    private SplitRes split() throws Exception {
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }

    /** 设置/获取 节点是否为叶子节点 */
    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if(isLeaf) {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)1;
        } else {
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)0;
        }
    }
    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte)1;
    }

    /** 设置/获取 节点中的Key的数量 */
    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short)noKeys), 0, raw.raw, raw.start+NO_KEYS_OFFSET, 2);
    }
    static int getRawNoKeys(SubArray raw) {
        return (int)Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start+NO_KEYS_OFFSET, raw.start+NO_KEYS_OFFSET+2));
    }

    /** 设置/获取 节点的兄弟节点UID */
    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start+SIBLING_OFFSET, 8);
    }
    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start+SIBLING_OFFSET, raw.start+SIBLING_OFFSET+8));
    }

    /** 设置/获取 节点中第k个儿子UID和Key */
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }
    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }
    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    /** 从第k个Son开始，复制剩余的Son和Key到另一个SubArray中（用来进行Node分裂） */
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NODE_HEADER_SIZE, from.end-offset);
    }

    /** 将第k个Son和Key之后的内容整体向后移动8+8位（方便插入新的键值对） */
    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start+NODE_HEADER_SIZE+(kth+1)*(8*2);
        int end = raw.start+NODE_SIZE-1;
        for(int i = end; i >= begin; i --) {
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }
}
