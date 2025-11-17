package top.wangbd.mydb.server.tm;

import top.wangbd.mydb.common.Error;
import top.wangbd.mydb.server.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 事务管理器接口
 * 负责管理数据库事务的生命周期，包括开始、提交、回滚以及状态查询
 */
public interface TransactionManager {
    /*** 开始一个新事务*/
    long begin();
    /*** 提交指定的事务*/
    void commit(long xid);
    /*** 回滚指定的事务*/
    void abort(long xid);
    /*** 检查事务是否处于活动状态*/
    boolean isActive(long xid);
    /*** 检查事务是否已提交*/
    boolean isCommitted(long xid);
    /*** 检查事务是否已回滚*/
    boolean isAborted(long xid);
    /*** 关闭事务管理器，释放相关资源*/
    void close();

    /** 创建一个新的事务管理文件，构造事务管理器实例 */
    public static TransactionManagerImpl create(String path) {
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
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

        // XID文件头，此时写入0
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }


    /** 读取事务管理文件，构造事务管理器实例 */
    public static TransactionManagerImpl open(String path) {
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
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

        return new TransactionManagerImpl(raf, fc);
    }
}
