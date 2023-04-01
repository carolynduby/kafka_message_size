package com.cloudera;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class KafkaMessageSize {

    public static void main(String[] args) {
	     if (args.length != 2) {
	         System.err.println("usage: KafkaMessageSize topic kafka_consumer_properties");
	         System.exit(-1);
         }

	     final String topic = args[0];
	     final String propertiesFile = args[1];

	     Properties kafkaProperties = new Properties();
        try {
            kafkaProperties.load(new FileInputStream(new File(propertiesFile)));
        } catch (IOException e) {
            System.err.println("Unable to read properties file due to '" + e.getMessage());
            System.exit(-1);
        }

        consume(kafkaProperties, topic);
    }


    private static void consume(Properties consumerProperties, String topic) {
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka_message_size_util");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(Collections.singletonList(topic));

            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMinutes(3));
            for (ConsumerRecord<String, byte[]> record : records){
                System.out.println("Read " + record.value().length + " bytes");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
