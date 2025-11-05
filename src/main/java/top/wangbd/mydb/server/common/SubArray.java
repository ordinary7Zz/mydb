package top.wangbd.mydb.server.common;

/**
 * 字节数组的子区间包装类
 * <p>
 * 用于表示一个字节数组的某个连续区间，避免数组的拷贝操作，提高性能。
 * 通过维护原始数组的引用以及起始和结束位置，实现对数组部分内容的访问。
 * </p>
 *
 * <p>
 * 使用场景：
 * 1. 在数据库系统中，经常需要从完整的数据页中提取某一段数据
 * 2. 避免频繁的数组拷贝操作，提高内存使用效率
 * 3. 实现零拷贝的数据访问模式
 * </p>
 *
 * @author wangbd
 */
public class SubArray {

    /**
     * 原始字节数组
     * 存储完整的数据内容
     */
    public byte[] raw;

    /**
     * 子区间的起始位置（包含）
     * 表示在原始数组中的起始索引
     */
    public int start;

    /**
     * 子区间的结束位置（不包含）
     * 表示在原始数组中的结束索引，区间为 [start, end)
     */
    public int end;

    /**
     * 构造一个字节数组的子区间
     *
     * @param raw 原始字节数组
     * @param start 子区间的起始位置（包含）
     * @param end 子区间的结束位置（不包含）
     */
    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
