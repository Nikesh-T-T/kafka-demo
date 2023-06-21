package com.nikesh.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {

        log.info("From ProducerDemo");

        // 1. create producer properties
        Properties prodecerProperties = new Properties();
        prodecerProperties.setProperty("key.serializer", StringSerializer.class.getName());
        prodecerProperties.setProperty("value.serializer", StringSerializer.class.getName());

        // 1.1. connect to localhost
//        prodecerProperties.setProperty("bootstrap.servers", "127.0.0.1.9092");

        // 1.2. connect to remote server
        prodecerProperties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        prodecerProperties.setProperty("security.protocol", "SASL_SSL");
        prodecerProperties.setProperty("sasl.mechanism", "PLAIN");
        prodecerProperties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='3YE3gAfFwwZa7VxWyOpw3t' password='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIzWUUzZ0FmRnd3WmE3VnhXeU9wdzN0Iiwib3JnYW5pemF0aW9uSWQiOjc0MTE4LCJ1c2VySWQiOjg2MjA0LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIyM2M1NDI0Mi01OTcxLTRiY2MtYjE3MC1hNTcwYTI0NmVlZGUifX0.E6-rhXj2kZjW57KyrbwwRXOwomThruW2R_A4tOm0tuI';");

        // 2. create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(prodecerProperties);

        // 3. create producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world!");

        // 4. send data (async)
        producer.send(producerRecord);

        // 5. flush and close producer

        // 5.1. flush -- tell producer to send all data and block until done (sync)
        producer.flush();

        // 5.2 close -- internally calls flush()
        producer.close();
    }

}
