package kafkaController;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaController {

    private KafkaProducer<String, String> producer;

    public KafkaController() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(kafkaProps);
    }

    public void sendMessage(String topic, String key, String value) {
        producer.send(new ProducerRecord<>(topic, key, value));
    }

    public void startGroup() {
        sendMessage("Command", null, "Start");
    }

    public void stopGroup() {
        sendMessage("Command", null, "Stop");
        close();
    }

    public void close() {
        producer.close();
    }

    public static void main(String[] args) {
        KafkaController controller = new KafkaController();
        System.out.println("Kafka Controller started. Type 'start' to start nodes or 'stop' to stop nodes.");

        java.util.Scanner scanner = new java.util.Scanner(System.in);
        while (true) {
            String command = scanner.nextLine();
            if ("start".equalsIgnoreCase(command)) {
                controller.startGroup();
                System.out.println("Start command sent to all nodes.");
            } else if ("stop".equalsIgnoreCase(command)) {
                controller.stopGroup();
                System.out.println("Stop command sent to all nodes.");
                break;
            } else {
                System.out.println("Unknown command. Type 'start' or 'stop'.");
            }
        }

        scanner.close();
    }
}