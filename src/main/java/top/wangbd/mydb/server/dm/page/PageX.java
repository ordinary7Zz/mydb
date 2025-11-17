package top.wangbd.mydb.server.dm.page;


import top.wangbd.mydb.server.dm.pageCache.PageCache;
import top.wangbd.mydb.server.utils.Parser;

import java.util.Arrays;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageX {
    private static final short OF_FREE = 0; // 空闲空间偏移量（FSO）的起始位置
    private static final short OF_DATA = 2; // 页面中数据部分的起始位置
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;  // 页面中可用于存储数据的最大空闲空间大小


    /*** 初始化普通页的原始数据*/
    public static byte[] initRaw() {
        // 创建一个新的字节数组，大小为页面大小（通常为8KB）。
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        // 将空闲空间偏移量（FSO）设置为数据部分的起始位置。
        setFSO(raw, OF_DATA);
        return raw;
    }

    /*** 将 raw 插入 page 中，返回插入位置*/
    public static short insert(Page pg, byte[] raw) {
        // raw 是一条DataItem的完整数据，包括有效位和长度
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFSO(pg.getData(), (short)(offset + raw.length));
        return offset;
    }

    /*** 设置新空闲位置的偏移 到 这一页的前两个字节
     * FSO：Free Space Offset*/
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    /*** 获取 page 的 FSO*/
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    /*** 根据这一页的前两个字节 获得 空闲位置的偏移*/
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /*** 获取页面的空闲空间大小*/
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
    }

    /*** 将 raw 插入 page 中的 offset 位置，并将 pg 的 offset 设置为较大的 offset
     * 用于在数据库崩溃后重新打开时，恢复例程直接插入数据使用。*/
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short)(offset+raw.length));
        }
    }

    /*** 将raw插入pg中的offset位置，不更新update
     * 用于在数据库崩溃后重新打开时，恢复例程修改数据使用。*/
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true); // 将pg的dirty标志设置为true，表示pg的数据已经被修改
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length); // 将raw的数据复制到pg的数据中的offset位置
    }
}
