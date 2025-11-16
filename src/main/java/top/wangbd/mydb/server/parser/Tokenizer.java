package top.wangbd.mydb.server.parser;

import top.wangbd.mydb.common.Error;

public class Tokenizer {
    private byte[] stat; // SQL statement
    private int pos; // 当前解析位置
    private String currentToken; // 当前Token
    private boolean flushToken;  // 是否需要刷新Token
    private Exception err; // 解析异常

    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    /** 读取当前Token，但不移动解析位置 */
    public String peek() throws Exception {
        if(err != null) {
            throw err;
        }
        if(flushToken) {
            String token = null;
            try {
                token = next();
            } catch(Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken;
    }

    /** 丢弃当前Token */
    public void pop() {
        flushToken = true;
    }

    /** 错误定位工具 ，用于在SQL解析出错时， 可视化地标记出错位置 */
    public byte[] errStat() {
        // 1. 创建新数组，比原SQL多3个字符（用于插入"<< "标记）
        byte[] res = new byte[stat.length+3];
        // 2. 复制错误位置之前的所有内容
        System.arraycopy(stat, 0, res, 0, pos);
        // 3. 在错误位置插入"<< "标记
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        // 4. 复制错误位置之后的所有内容
        System.arraycopy(stat, pos, res, pos+3, stat.length-pos);

        return res;
    }

    /** 读取下一个Token */
    private String next() throws Exception {
        if(err != null) {
            throw err;
        }
        return nextMetaState();
    }

    /** 读取一个完整的Token */
    private String nextMetaState() throws Exception {
        while(true) {
            // 循环跳过空白字符
            Byte b = peekByte();
            if(b == null) {
                // 到达结尾，返回空字符串
                return "";
            }
            if(!isBlank(b)) {
                break;
            }
            popByte();
        }
        byte b = peekByte();
        if(isSymbol(b)) {
            // 如果是符号，直接返回该符号作为Token
            popByte();
            return new String(new byte[]{b});
        } else if(b == '"' || b == '\'') {
            // 读取单引号或双引号包裹的字符串
            return nextQuoteState();
        } else if(isAlphaBeta(b) || isDigit(b)) {
            // 读取由字母或数字组成的Token
            return nextTokenState();
        } else {
            err = Error.InvalidCommandException;
            throw err;
        }
    }

    /** 读取SQL statement 的pos位置的字节，但不移动解析位置 */
    private Byte peekByte() {
        if(pos == stat.length) {
            return null;
        }
        return stat[pos];
    }

    /** 移动解析位置 */
    private void popByte() {
        pos ++;
        if(pos > stat.length) {
            pos = stat.length;
        }
    }

    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }

    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
                b == ',' || b == '(' || b == ')');
    }

    /** 读取单引号或双引号包裹的字符串 */
    private String nextQuoteState() throws Exception {
        byte quote = peekByte();
        popByte();
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                err = Error.InvalidCommandException;
                throw err;
            }
            // 读到对称的引号就返回
            if(b == quote) {
                popByte();
                break;
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
        return sb.toString();
    }

    /** 读取由字母或数字组成的Token */
    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                if(b != null && isBlank(b)) {
                    popByte();
                }
                return sb.toString();
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
    }

    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }
}
