package com.ztwu.demo.serializable;

import java.io.*;
import java.lang.*;

import com.ztwu.demo.avro.User;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.DataFileReader;

public class SerializableTest {

    public static void main(String args[]) {

        User user1 = new User();
        user1.setName("Arway");
        user1.setFavoriteNumber(3);
        user1.setFavoriteColor("green");
        User user2 = new User("Ben", 7, "red");
        //construct with builder
        User user3 = User.newBuilder().setName("Charlie").setFavoriteColor("blue").setFavoriteNumber(100).build();

        //Serialize user1, user2 and user3 to disk
        File file = new File("data/users.avro");
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        try {
            dataFileWriter.create(user1.getSchema(), new File("data/users.avro"));
            for(int i = 0; i<10; i++){
                dataFileWriter.append(user1);
                dataFileWriter.append(user2);
                dataFileWriter.append(user3);
            }
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Deserialize Users from dist
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = null;
        try {
            dataFileReader = new DataFileReader<User>(file, userDatumReader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        User user = null;

        File textfile = new File("data/user.txt");
        BufferedWriter bw = null;
        try {
             bw = new BufferedWriter(new FileWriter(textfile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            while (dataFileReader.hasNext()) {
                user = dataFileReader.next(user);
                System.out.println(user.toString()+"---");
                bw.write(user.toString());
                bw.flush();
            }
            bw.close();
        } catch (IOException e) {
        }
    }
}
