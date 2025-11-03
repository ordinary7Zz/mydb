package top.wangbd.mydb.server.utils;

/**
 * 解析字符串结果类
 * 包含解析出的字符串和下一个字符的索引位置
 */
public class ParseStringRes {
    public String str;
    public int next;

    public ParseStringRes(String str, int next) {
        this.str = str;
        this.next = next;
    }
}
