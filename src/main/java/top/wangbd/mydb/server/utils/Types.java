package top.wangbd.mydb.server.utils;

public class Types {
    /** 将页号和页内偏移转换为全局唯一地址UID*/
    public static long addressToUid(int pgno, short offset) {
        long u0 = (long)pgno;
        long u1 = (long)offset;
        return u0 << 32 | u1;
    }
}
