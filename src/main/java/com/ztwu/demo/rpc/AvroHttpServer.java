package com.ztwu.demo.rpc;

import java.io.File;

import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.generic.GenericResponder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 动态rpc
 */
public class AvroHttpServer extends GenericResponder {

    private static Log log = LogFactory.getLog(AvroHttpServer.class);

    public AvroHttpServer(Protocol protocol) {
        super(protocol);
    }

    public Object respond(Message message, Object request) throws Exception {
        GenericRecord req = (GenericRecord) request;
        GenericRecord reMessage = null;
        if (message.getName().equals("sayHello")) {
            Object name = req.get("name");
            //  do something...
            //取得返回值的类型
            reMessage = new GenericData.Record(super.getLocal().getType("nameMessage"));
            //直接构造回复
            reMessage.put("name", "Hello, " + name.toString());
            log.info(reMessage);
        }
        return reMessage;
    }

    public static void main(String[] args) throws Exception {
        int port = 8088;
        try {
            Server server = new HttpServer(
                    new AvroHttpServer(Protocol.parse(
                            new File("D:\\data\\idea\\java\\private\\github-self-workspace\\avro-test\\src\\main\\resources\\test1.avro"))),
                    port);
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}