package top.wangbd.mydb.transport;

public class Packager {
    private Transporter transpoter;
    private Encoder encoder;

    public Packager(Transporter transpoter, Encoder encoder) {
        this.transpoter = transpoter;
        this.encoder = encoder;
    }

    /** 发送数据包 */
    public void send(Package pkg) throws Exception {
        byte[] data = encoder.encode(pkg);
        transpoter.send(data);
    }

    /** 接收数据包 */
    public Package receive() throws Exception {
        byte[] data = transpoter.receive();
        return encoder.decode(data);
    }

    public void close() throws Exception {
        transpoter.close();
    }
}
