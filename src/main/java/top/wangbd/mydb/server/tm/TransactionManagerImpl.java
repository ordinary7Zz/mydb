package top.wangbd.mydb.server.tm;

import top.wangbd.mydb.server.common.Error;
import top.wangbd.mydb.server.utils.Panic;
import top.wangbd.mydb.server.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager{

    // XID 文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;
    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE   = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED  = 2;
    // 超级事务，永远为 commited 状态
    public static final long SUPER_XID = 0;
    // XID 文件后缀
    static final String XID_SUFFIX = ".xid";

    // XID 文件
    private RandomAccessFile file;
    // 文件通道
    private FileChannel fc;
    // 事务ID计数器
    private long xidCounter; // XID文件头八个字节
    // 保护 xidCounter 的锁
    private Lock counterLock;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        // 获取文件实际长度
        try {
            fileLen = file.length();
        } catch (IOException e1) {
            Panic.panic(Error.BadXIDFileException);
        }

        // 文件长度至少要大于文件头长度
        if(fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        // 读取文件头
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        // 读取文件头内容到buf
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 解析事务ID计数器
        this.xidCounter = Parser.parseLong(buf.array());
        // 计算理论文件结束位置
        long end = getXidPosition(this.xidCounter + 1);
        // 对比实际文件长度
        if(end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }

    }

    /**
     * 根据事务xid取得其在xid文件中对应的位置
     */
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid-1) * XID_FIELD_SIZE;
    }

    /**
     * 更新xid事务的状态
     */
    private void updateXID(long xid, byte status) {
        // 计算xid在文件中的偏移位置
        long offset = getXidPosition(xid);
        // 构造状态字节数组
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        // 包装为ByteBuffer
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        // 写入文件
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 强制写入磁盘
        try {
            fc.force(false); // 强制刷新文件内容到磁盘,不强制刷新元数据
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 将XID加一，并更新XID Header
     */
    private void incrXIDCounter() {
        xidCounter++;
        // 更新XID文件头
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 检测XID事务是否处于status状态
     */
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    /**
     * 开启一个事务，并返回事务的XID
     */
    @Override
    public long begin() {
        counterLock.lock();
        try {
            // 分配新的XID
            long xid = xidCounter + 1;
            // 将XID状态设置为ACTIVE
            updateXID(xid, FIELD_TRAN_ACTIVE);
            // 更新XID计数器
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    /**
     * 提交XID事务
     */
    @Override
    public void commit(long xid) {
        // 将XID状态设置为COMMITTED
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    /**
     * 回滚XID事务
     */
    @Override
    public void abort(long xid) {
        // 将XID状态设置为ABORTED
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    /**
     * 检查事务是否处于活动状态
     */
    @Override
    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    /**
     * 检查事务是否已提交
     */
    @Override
    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    /**
     * 检查事务是否已回滚
     */
    @Override
    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    /**
     * 关闭事务管理器，释放相关资源
     */
    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
