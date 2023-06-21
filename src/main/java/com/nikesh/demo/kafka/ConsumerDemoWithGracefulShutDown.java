package com.nikesh.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithGracefulShutDown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithGracefulShutDown.class);

    public static void main(String[] args) {

        log.info("From ConsumerDemo");
        String groupId = "my-java-application";
        String topic = "demo_java";

        // 1. create consumer properties
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.setProperty("value.deserializer", StringDeserializer.class.getName());
        consumerProperties.setProperty("group.id", groupId);
        consumerProperties.setProperty("auto.offset.reset", "earliest");

        // 1.1. connect to localhost
        //consumerProperties.setProperty("bootstrap.servers", "127.0.0.1.9092");

        // 1.2. connect to remote server
        consumerProperties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        consumerProperties.setProperty("security.protocol", "SASL_SSL");
        consumerProperties.setProperty("sasl.mechanism", "PLAIN");
        consumerProperties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='3YE3gAfFwwZa7VxWyOpw3t' password='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIzWUUzZ0FmRnd3WmE3VnhXeU9wdzN0Iiwib3JnYW5pemF0aW9uSWQiOjc0MTE4LCJ1c2VySWQiOjg2MjA0LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIyM2M1NDI0Mi01OTcxLTRiY2MtYjE3MC1hNTcwYTI0NmVlZGUifX0.E6-rhXj2kZjW57KyrbwwRXOwomThruW2R_A4tOm0tuI';");

        // 2. create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);

        // 3. setup config for the graceful shutdown of the consumer

        // 3.1 get a reference to the main thread
        Thread mainThread = Thread.currentThread();

        // 3.2 add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown let's exit by calling consumer.wakeup()...");
            // this will configure the consumer so that next time consumer.poll() will throw a WakeupException
            consumer.wakeup();

            // join the main thread to allow the execution of the remaining code in this page
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        try {
            // 3. subscribe to list of topics
            consumer.subscribe(Collections.singleton(topic));

            // 4. poll data
            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("Key: {} | Value: {}", consumerRecord.key(), consumerRecord.value());
                    log.info("Partition: {} | Offset: {}", consumerRecord.partition(), consumerRecord.offset());
                }
            }
        } catch (WakeupException ex) {
            log.info("Consumer is starting to shut down");
        } catch (Exception ex) {
            log.error("Unexpected exception", ex);
        } finally {
            // close the consumer, this will gracefully close any connection to kafka and commit the offsets
            consumer.close();
            log.info("Consumer is now gracefully shutdown");
        }

    }

}
