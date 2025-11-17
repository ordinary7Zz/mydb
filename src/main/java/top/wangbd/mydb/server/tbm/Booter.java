package top.wangbd.mydb.server.tbm;

import top.wangbd.mydb.common.Error;
import top.wangbd.mydb.server.utils.Panic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * 记录第一个表的uid
 */
public class Booter {
    public static final String BOOTER_SUFFIX = ".bt";
    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";

    String path;
    File file;

    /** 创建booter文件 */
    public static Booter create(String path) {
        removeBadTmp(path);
        File f = new File(path+BOOTER_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    /** 打开booter文件 */
    public static Booter open(String path) {
        removeBadTmp(path);
        File f = new File(path+BOOTER_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    /** 读取booter文件内容（第一个表的uid） */
    public byte[] load() {
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf;
    }

    /** 更新booter文件内容（第一个表的uid） */
    public void update(byte[] data) {
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        try {
            tmp.createNewFile();
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!tmp.canRead() || !tmp.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        try(FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);
            out.flush();
        } catch(IOException e) {
            Panic.panic(e);
        }
        try {
            Files.move(tmp.toPath(), new File(path+BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch(IOException e) {
            Panic.panic(e);
        }
        file = new File(path+BOOTER_SUFFIX);
        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
    }

    private static void removeBadTmp(String path) {
        new File(path+BOOTER_TMP_SUFFIX).delete();
    }

    private Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }
}
