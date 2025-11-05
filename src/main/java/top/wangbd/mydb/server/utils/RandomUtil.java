package top.wangbd.mydb.server.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * 随机数工具类
 * 提供生成随机字节数组的功能，使用 SecureRandom 确保生成的随机数具有密码学强度。
 */
public class RandomUtil {

    /**
     * 生成指定长度的随机字节数组
     * 使用 SecureRandom 生成密码学强度的随机数，适用于安全性要求较高的场景。
     * SecureRandom 生成的随机数具有不可预测性，比普通的 Random 更安全。
     */
    public static byte[] randomBytes(int length) {
        Random r = new SecureRandom();
        byte[] buf = new byte[length];
        r.nextBytes(buf); // 用随机生成的字节填充整个字节数组 buf
        return buf;
    }
}
