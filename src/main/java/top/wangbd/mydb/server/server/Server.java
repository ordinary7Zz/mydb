package top.wangbd.mydb.server.server;

import top.wangbd.mydb.server.tbm.TableManager;
import top.wangbd.mydb.transport.Encoder;
import top.wangbd.mydb.transport.Package;
import top.wangbd.mydb.transport.Packager;
import top.wangbd.mydb.transport.Transporter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Server {
    private int port;
    TableManager tbm;

    public Server(int port, TableManager tbm) {
        this.port = port;
        this.tbm = tbm;
    }

    public void start() {
        ServerSocket ss = null;
        try {
            // 创建服务器Socket，监听指定端口
            ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("Server listen to port: " + port);
        // 创建线程池，处理客户端连接
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(10, 20, 1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            while(true) {
                // 如果有客户端连接，创建一个Socket对象
                Socket socket = ss.accept();
                // 为该连接创建一个处理任务，并提交给线程池执行
                Runnable worker = new HandleSocket(socket, tbm);
                tpe.execute(worker);
            }
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            try {
                ss.close();
            } catch (IOException ignored) {}
        }
    }

}

/** 处理每一个连接 */
class HandleSocket implements Runnable {
    private Socket socket;
    private TableManager tbm;

    public HandleSocket(Socket socket, TableManager tbm) {
        this.socket = socket;
        this.tbm = tbm;
    }


    @Override
    public void run() {
        // 1. 记录客户端连接信息
        InetSocketAddress address = (InetSocketAddress)socket.getRemoteSocketAddress();
        System.out.println("Establish connection: " + address.getAddress().getHostAddress()+":"+address.getPort());

        //  2. 初始化通信组件
        Packager packager = null;
        try {
            Transporter t = new Transporter(socket);
            Encoder e = new Encoder();
            packager = new Packager(t, e);
        } catch(IOException e) {
            e.printStackTrace();
            try {
                socket.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return;
        }
        Executor exe = new Executor(tbm);

        // 3. 循环处理客户端SQL请求
        while(true) {
            Package pkg = null;
            try {
                pkg = packager.receive();
            } catch(Exception e) {
                break;
            }
            byte[] sql = pkg.getData();
            byte[] result = null;
            Exception e = null;
            try {
                result = exe.execute(sql);
            } catch (Exception e1) {
                e = e1;
                e.printStackTrace();
            }
            pkg = new Package(result, e);
            try {
                packager.send(pkg);
            } catch (Exception e1) {
                e1.printStackTrace();
                break;
            }
        }
        exe.close();
        try {
            packager.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
