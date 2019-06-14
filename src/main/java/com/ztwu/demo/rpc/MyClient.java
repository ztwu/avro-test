package com.ztwu.demo.rpc;

import com.ztwu.demo.avro.Mail;
import com.ztwu.demo.avro.Message;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * created with idea
 * user:ztwu
 * date:2019/6/14
 * description
 */
public class MyClient {

    public static void main(String[] args) throws IOException {

        args = new String[]{"1","2","3"};

        NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(65111));
        Mail proxy = (Mail) SpecificRequestor.getClient(Mail.class, client);
        System.out.println("Client built, got proxy");

        Message message = new Message();
        message.setTo(new Utf8(args[0]));
        message.setFrom(new Utf8(args[1]));
        message.setBody(new Utf8(args[2]));
        System.out.println("Calling proxy.send with message:  " + message.toString());
        System.out.println("Result: " + proxy.send(message));

        client.close();
    }

}
