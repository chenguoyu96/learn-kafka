package org.chenguoyu.kafka.consumer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.chenguoyu.kafka.procuder.Company;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class MyDeserializer implements Deserializer<Company> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null)
            return null;
        if (data.length < 8) {
            throw new SerializationException();
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen, addressLen;

        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameLen);
        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);

        try {
            String name = new String(nameBytes, "UTF-8");
            String address = new String(addressBytes, "UTF-8");
            return new Company(name, address);
        } catch (UnsupportedEncodingException e) {
            throw  new SerializationException("反序列化异常");
        }
    }

    @Override
    public void close() {

    }
}
