package top.wangbd.mydb.server.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.primitives.Bytes;

/**
 * 数据类型转换工具类
 */
public class Parser {

    /**
     * 将short类型转换为字节数组
     * @param value 要转换的short值
     * @return 转换后的字节数组（2字节）
     */
    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    /**
     * 将字节数组解析为short类型
     * @param buf 包含short值的字节数组（至少2字节）
     * @return 解析后的short值
     */
    public static short parseShort(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }

    /**
     * 将int类型转换为字节数组
     * @param value 要转换的int值
     * @return 转换后的字节数组（4字节）
     */
    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    /**
     * 将字节数组解析为int类型
     * @param buf 包含int值的字节数组（至少4字节）
     * @return 解析后的int值
     */
    public static int parseInt(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 4);
        return buffer.getInt();
    }

    /**
     * 将字节数组解析为long类型
     * @param buf 包含long值的字节数组（至少8字节）
     * @return 解析后的long值
     */
    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    /**
     * 将long类型转换为字节数组
     * @param value 要转换的long值
     * @return 转换后的字节数组（8字节）
     */
    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    /**
     * 从字节数组中解析字符串
     * 格式：前4字节为字符串长度，后续为字符串内容
     * @param raw 包含字符串的字节数组
     * @return ParseStringRes对象，包含解析出的字符串和消耗的总字节数
     */
    public static ParseStringRes parseString(byte[] raw) {
        int length = parseInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4+length));
        return new ParseStringRes(str, length+4);
    }

    /**
     * 将字符串转换为字节数组
     * 格式：前4字节为字符串长度，后续为字符串内容
     * @param str 要转换的字符串
     * @return 转换后的字节数组（长度信息4字节 + 字符串字节）
     */
    public static byte[] string2Byte(String str) {
        byte[] l = int2Byte(str.length());
        return Bytes.concat(l, str.getBytes());
    }

    /**
     * 将字符串转换为唯一标识符（UID）
     * 使用字符串哈希算法生成long类型的唯一ID
     * @param key 要转换的字符串
     * @return 生成的唯一标识符
     */
    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for(byte b : key.getBytes()) {
            res = res * seed + (long)b;
        }
        return res;
    }

}
