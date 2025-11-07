package top.wangbd.mydb.server.dm.Logger;

import top.wangbd.mydb.common.Error;
import top.wangbd.mydb.server.utils.Panic;
import top.wangbd.mydb.server.utils.Parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface Logger {
    /*** 写入日志数据*/
    void log(byte[] data);
    /*** 截断日志文件到指定位置*/
    void truncate(long x) throws Exception;
    /*** 获取下一条日志数据*/
    byte[] next();
    /*** 将日志读取指针重置到文件开头
     * 用于重新遍历所有日志记录*/
    void rewind();
    /*** 关闭日志文件，释放相关资源*/
    void close();

    /*** 创建日志文件*/
    public static Logger create(String path) {
        File f = new File(path+LoggerImpl.LOG_SUFFIX);
        try {
            // 尝试在指定路径创建日志文件，如果文件已存在 createNewFile() 返回 false
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

        // 初始化 [XChecksum] 的字节大小（写入4个字节的0）
        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new LoggerImpl(raf, fc, 0);
    }

    /*** 打开已有的日志文件*/
    public static Logger open(String path) {
        File f = new File(path+LoggerImpl.LOG_SUFFIX);
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
        LoggerImpl lg = new LoggerImpl(raf, fc);
        lg.init();

        return lg;
    }


}
