package top.wangbd.mydb.server.tbm;

import com.google.common.primitives.Bytes;
import top.wangbd.mydb.common.Error;
import top.wangbd.mydb.server.parser.statement.*;
import top.wangbd.mydb.server.tm.TransactionManagerImpl;
import top.wangbd.mydb.server.utils.Panic;
import top.wangbd.mydb.server.utils.ParseStringRes;
import top.wangbd.mydb.server.utils.Parser;

import java.util.*;

/**
 * 表结构：mydb的所有表使用链表连接起来，每个表包含表名、下一个表的uid以及字段列表，新增一个表时，将新表插入到链表头部
 * 格式：
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 */
public class Table {
    TableManager tbm;
    long uid;
    String name;
    byte status;
    long nextUid;
    List<Field> fields = new ArrayList<>();

    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    /** 根据表的uid，读取并解析表结构，返回Table实例 */
    public static Table loadTable(TableManager tbm, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);
    }

    /**
     * 创建一个Table实例并持久化，新的Table会被插入到表链表的头部
     * @param nextUid 当前Table链表的头部uid
     */
    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table tb = new Table(tbm, create.tableName, nextUid);
        // 将索引字段数组转换成为HashSet
        Set<String> indexSet = new HashSet<>(Arrays.asList(create.index));
        // 遍历字段定义，创建字段并标记是否索引
        for(int i = 0; i < create.fieldName.length; i ++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = indexSet.contains(fieldName);
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed));
        }

        return tb.persistSelf(xid);
    }

    /** 数据库插入操作
     *  mydb的插入只允许插入一行数据，且必须提供所有字段的值
     * */
    public void insert(long xid, Insert insert) throws Exception {
        // 将插入的一行数据转换成为[字段名，字段值]的Map形式的记录
        Map<String, Object> entry = string2Entry(insert.values);
        byte[] raw = entry2Raw(entry);
        // 将记录插入到数据源中，获取对应的uid
        long uid = ((TableManagerImpl)tbm).vm.insert(xid, raw);
        // 遍历字段列表，向有索引的字段插入索引数据
        for (Field field : fields) {
            if(field.isIndexed()) {
                field.insert(entry.get(field.fieldName), uid);
            }
        }
    }

    /** 数据库删除操作 */
    public int delete(long xid, Delete delete) throws Exception {
        // 解析where条件，获取符合条件的uid列表
        List<Long> uids = parseWhere(delete.where);
        int count = 0;
        // 遍历uid列表，逐条删除
        for (Long uid : uids) {
            if(((TableManagerImpl)tbm).vm.delete(xid, uid)) {
                count ++;
            }
        }
        return count;
    }

    /** 数据库更新操作
     *  mydb的更新只允许更新单个字段，且必须提供新的字段值
     *  mydb的更新操作实际上是删除旧记录，插入新记录的过程
     * */
    public int update(long xid, Update update) throws Exception {
        // 解析where条件，获取符合条件的uid列表
        List<Long> uids = parseWhere(update.where);
        Field fd = null;
        // 查找需要唯一需要更新的字段
        for (Field f : fields) {
            if(f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }
        if(fd == null) {
            throw Error.FieldNotFoundException;
        }
        // 将新的字段值转换成为对应类型的对象
        Object value = fd.string2Value(update.value);
        int count = 0;
        for (Long uid : uids) {
            // 获取旧记录的二进制数据
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;

            // 删除旧记录
            ((TableManagerImpl)tbm).vm.delete(xid, uid);

            Map<String, Object> entry = parseEntry(raw);
            // 更新指定字段的值
            entry.put(fd.fieldName, value);
            // 将更新后的记录重新转换成为二进制数据
            raw = entry2Raw(entry);
            // 插入新记录，获取新的uid
            long uuid = ((TableManagerImpl)tbm).vm.insert(xid, raw);

            count ++;

            // 更新所有索引字段的索引数据
            for (Field field : fields) {
                if(field.isIndexed()) {
                    field.insert(entry.get(field.fieldName), uuid);
                }
            }
        }
        return count;
    }

    /** 数据库查询操作
     *  返回结果是字符串形式，每行记录占一行
     * */
    public String read(long xid, Select read) throws Exception {
        // 解析where条件，获取符合条件的uid列表
        List<Long> uids = parseWhere(read.where);
        StringBuilder sb = new StringBuilder();
        for (Long uid : uids) {
            // 读取符合条件（版本可见性）的记录的二进制数据
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;
            Map<String, Object> entry = parseEntry(raw);
            sb.append(printEntry(entry)).append("\n");
        }
        return sb.toString();
    }


    /** 将二进制数据解析为Table对象 */
    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = Parser.parseString(raw);
        // 解析表名
        name = res.str;
        position += res.next;
        // 解析下一个表的uid
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        position += 8;

        // 解析字段列表
        while(position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
            position += 8;
            fields.add(Field.loadField(this, uid));
        }
        return this;
    }

    /** 将Table对象持久化到数据源中 */
    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for(Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
        }
        uid = ((TableManagerImpl)tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    /** 解析Where条件，返回符合条件的uid列表。如果where为空，选择第一个有索引的字段进行全表扫描 */
    private List<Long> parseWhere(Where where) throws Exception {
        long l0=0, r0=0, l1=0, r1=0;
        boolean single = false;
        Field fd = null;
        // 如果where为空，则选择第一个有索引的字段进行全表扫描
        if(where == null) {
            for (Field field : fields) {
                if(field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        } else {
            for (Field field : fields) {
                if(field.fieldName.equals(where.singleExp1.field)) {
                    if(!field.isIndexed()) {
                        throw Error.FieldNotIndexedException;
                    }
                    fd = field;
                    break;
                }
            }
            if(fd == null) {
                throw Error.FieldNotFoundException;
            }
            CalWhereRes res = calWhere(fd, where);
            l0 = res.l0; r0 = res.r0;
            l1 = res.l1; r1 = res.r1;
            single = res.single;
        }
        // 这里就限制了每个表必须有一个索引字段，否则无法进行查询
        List<Long> uids = fd.search(l0, r0);
        if(!single) {
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }

    class CalWhereRes {
        long l0, r0, l1, r1;
        boolean single;
    }

    /** 计算Where条件对应的范围
     *  mydb的where只支持单字段的简单条件表达式，且该字段必须是索引字段
     * */
    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch(where.logicOp) {
            case "":
                res.single = true;
                FieldCalRes r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                break;
            case "or":
                res.single = false;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                break;
            case "and":
                res.single = true;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                // 计算交集（因为where的字段只能是一个，所以可以计算交集）
                if(res.l1 > res.l0) res.l0 = res.l1;
                if(res.r1 < res.r0) res.r0 = res.r1;
                break;
            default:
                throw Error.InvalidLogOpException;
        }
        return res;
    }

    /** 将一行数据转换成为[字段名，字段值]的Map形式的记录 */
    private Map<String, Object> string2Entry(String[] values) throws Exception {
        if(values.length != fields.size()) {
            throw Error.InvalidValuesException;
        }
        Map<String, Object> entry = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            entry.put(f.fieldName, v);
        }
        return entry;
    }

    /** 将一行记录转换成为二进制数据 */
    private byte[] entry2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    /** 将二进制数据的一行记录，以键值对Map的形式解析 */
    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        for (Field field : fields) {
            Field.ParseValueRes r = field.parserValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, r.v);
            pos += r.shift;
        }
        return entry;
    }

    /** 将一行记录以字符串形式打印 */
    private String printEntry(Map<String, Object> entry) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            sb.append(field.printValue(entry.get(field.fieldName)));
            if(i == fields.size()-1) {
                sb.append("]");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for(Field field : fields) {
            sb.append(field.toString());
            if(field == fields.get(fields.size()-1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
