package top.wangbd.mydb.server.tbm;

import com.google.common.primitives.Bytes;
import top.wangbd.mydb.common.Error;
import top.wangbd.mydb.server.im.BPlusTree;
import top.wangbd.mydb.server.parser.statement.SingleExpression;
import top.wangbd.mydb.server.tm.TransactionManagerImpl;
import top.wangbd.mydb.server.utils.Panic;
import top.wangbd.mydb.server.utils.ParseStringRes;
import top.wangbd.mydb.server.utils.Parser;

import java.util.Arrays;
import java.util.List;

/**
 * 字段类
 * 格式：[FieldName][TypeName][IndexUid]
 */
public class Field {
    long uid; // Field的uid
    private Table tb; // Field所属的表
    String fieldName; // 字段名
    String fieldType; // 字段类型
    private long index; // 索引的uid，如果无索引则为0
    private BPlusTree bt; // 索引对应的BPlusTree实例

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
    }

    /** 根据表对象的VM，通过读取uid，获得Field实例对象 */
    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        return new Field(uid, tb).parseSelf(raw);
    }

    /** 创建一个Field实例，并持久化 */
    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception {
        typeCheck(fieldType);
        Field f = new Field(tb, fieldName, fieldType, 0);
        if(indexed) {
            // 如果需要索引，则创建BPlusTree，并将index和bt赋值给Field实例
            long index = BPlusTree.create(((TableManagerImpl)tb.tbm).dm);
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            f.index = index;
            f.bt = bt;
        }
        f.persistSelf(xid);
        return f;
    }

    /** 向字段的BPlusTree索引中插入键值对 */
    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Uid(key);
        bt.insert(uKey, uid);
    }

    /** 在字段的BPlusTree索引中搜索指定范围的uid列表
     *  输入参数为范围的左右边界值
     * */
    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
    }

    class ParseValueRes {
        Object v;
        int shift;
    }

    /** 在二进制的行数据中，解析出属于当前Field类型的字段值
     *  因为一行数据的所有字段值都是连续存储的，所以需要返回解析后数据的偏移量
     * */
    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch(fieldType) {
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }

    /** 根据单表达式，计算出对应的key的范围（key也是用uid函数计算的） */
    public FieldCalRes calExp(SingleExpression exp) throws Exception {
        Object v = null;
        FieldCalRes res = new FieldCalRes();
        switch(exp.compareOp) {
            case "<":
                res.left = 0;
                v = string2Value(exp.value);
                res.right = value2Uid(v);
                if(res.right > 0) {
                    res.right --;
                }
                break;
            case "=":
                v = string2Value(exp.value);
                res.left = value2Uid(v);
                res.right = res.left;
                break;
            case ">":
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                res.left = value2Uid(v) + 1;
                break;
        }
        return res;
    }

    public boolean isIndexed() {
        return index != 0;
    }

    /** 将字段值转换为uid，用于BPlusTree的索引
     *  mydb计算字段值的哈希函数也使用uid的计算函数
     * */
    public long value2Uid(Object key) {
        long uid = 0;
        switch(fieldType) {
            case "string":
                uid = Parser.str2Uid((String)key);
                break;
            case "int32":
                int uint = (int)key;
                return (long)uint;
            case "int64":
                uid = (long)key;
                break;
        }
        return uid;
    }

    /** 把解析出的字符串，根据Filed的类型转换为对应的值对象 */
    public Object string2Value(String str) {
        switch(fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    /** 解析Field的raw数据，并返回Field对象本身
     *  根据Field的格式，其为两个连续的字符串和一个long值
     * */
    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        // 解析字段名
        fieldName = res.str;
        position += res.next;
        res = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        // 解析字段类型
        fieldType = res.str;
        position += res.next;
        // 解析索引uid，如果uid=0，则表示无索引，否则加载BPlusTree给Field实例
        this.index = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        if(index != 0) {
            try {
                bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            } catch(Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    /** 将Field实例数据持久化到数据源中 */
    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(fieldName);
        byte[] typeRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(index);
        this.uid = ((TableManagerImpl)tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    /** 字段类型检查 */
    private static void typeCheck(String fieldType) throws Exception {
        if(!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;
        }
    }

    /** 将字段值转换为二进制表示 */
    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch(fieldType) {
            case "int32":
                raw = Parser.int2Byte((int)v);
                break;
            case "int64":
                raw = Parser.long2Byte((long)v);
                break;
            case "string":
                raw = Parser.string2Byte((String)v);
                break;
        }
        return raw;
    }


    public String printValue(Object v) {
        String str = null;
        switch(fieldType) {
            case "int32":
                str = String.valueOf((int)v);
                break;
            case "int64":
                str = String.valueOf((long)v);
                break;
            case "string":
                str = (String)v;
                break;
        }
        return str;
    }

    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(", ")
                .append(fieldType)
                .append(index!=0?", Index":", NoIndex")
                .append(")")
                .toString();
    }
}
