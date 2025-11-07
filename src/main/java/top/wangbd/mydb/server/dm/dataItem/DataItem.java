package top.wangbd.mydb.server.dm.dataItem;

import com.google.common.primitives.Bytes;
import top.wangbd.mydb.server.common.SubArray;
import top.wangbd.mydb.server.dm.DataManagerImpl;
import top.wangbd.mydb.server.dm.page.Page;
import top.wangbd.mydb.server.utils.Parser;
import top.wangbd.mydb.server.utils.Types;

import java.util.Arrays;

/**
 *  [ValidFlag] [DataSize] [Data]
 *  ValidFlag 1字节，0为合法，1为非法
 *  DataSize  2字节，标识Data的长度
 */
public interface DataItem {
    SubArray data(); // 获取data部分的数据

    void before();   // 修改前的操作，保存旧数据
    void unBefore(); // 撤销修改前的操作，恢复旧数据
    void after(long xid); // 修改后的操作，标记页面为脏页
    void release(); // 释放锁等资源

    void lock(); // 获取写锁
    void unlock(); // 释放写锁
    void rLock(); // 获取读锁
    void rUnLock();// 释放读锁
    Page page(); // 获取所属页面
    long getUid();// 获取唯一标识
    byte[] getOldRaw();// 获取修改前的旧数据
    SubArray getRaw();// 获取整个DataItem的原始数据

    /** 将raw数据封装成DataItem格式，前面加上有效位和长度 */
    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short)raw.length);
        return Bytes.concat(valid, size, raw);
    }

    /** 从页面的offset处解析出dataitem
     *  感觉这个方法应该放在DataItemImpl里面更合适，因为dm没有在方法里使用到，只是作为参数传给DataItem的构造函数
     * */
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm){
        byte[] raw = pg.getData();
        // 解析dataitem的数据长度
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset+DataItemImpl.OF_SIZE, offset+DataItemImpl.OF_DATA));
        // 计算dataitem的总长度，size + 3（有效位1个字节，size2个字节）
        short length = (short)(size + DataItemImpl.OF_DATA);
        // 计算uid
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset+length), new byte[length], pg, uid, dm);
    }

    /** 将DataItem置为无效，有效位设为1 */
    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }


}
