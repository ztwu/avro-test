package com.ztwu.demo.rpc;

import com.ztwu.demo.avro.Mail;
import com.ztwu.demo.avro.Message;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * created with idea
 * user:ztwu
 * date:2019/6/14
 * description 静态rpc
 */
public class MyServer {

    public static class MailImpl implements Mail {

        @Override
        public Utf8 send(Message message) {
            System.out.println("Sending message");
            return new Utf8("Sending message to " + message.getTo().toString()
                    + " from " + message.getFrom().toString()
                    + " with body " + message.getBody().toString());
        }

    }

    public static void main(String[] args) throws IOException {
        System.out.println("Starting server");
        NettyServer server = new NettyServer(new SpecificResponder(Mail.class, new MailImpl()), new InetSocketAddress(65111));
        System.out.println("Server started");
    }

}