package ru.nkargin.kafkatest;

import ru.nkargin.kafkatest.consumer.TestKafkaConsumer;
import ru.nkargin.kafkatest.producer.TestKafkaProducer;

public class Main {

    public static void main(String[] args) {
        new TestKafkaConsumer().start();
        new TestKafkaProducer().start();
    }

}
