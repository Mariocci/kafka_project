package sensorNode;

import java.time.Duration;
import java.net.DatagramPacket;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.concurrent.ConcurrentHashMap;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class Sensor {
    private String id;
    private int port;
    private LinkedList<Reading> readings = new LinkedList<>();
    private EmulatedSystemClock clock;
    private UDPCommunication udpComm;
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;
    private Map<String, Integer> nodeIndexMap = new HashMap<>();
    private long[] vectorClock = new long[0];
    private final Set<String> receivedPackets = ConcurrentHashMap.newKeySet();

    private final ConcurrentHashMap<String, String> registeredNodes = new ConcurrentHashMap<>();

    private void setupKafka() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, id);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(java.util.Arrays.asList("Command", "Register"));
    }

    public void initialize(String nodeId, int port) {
        this.id = nodeId;
        this.port = port;

        registerNode(id);
        clock = new EmulatedSystemClock();

        try {
            udpComm = new UDPCommunication(port, 0.3, 1000);
            processReceivedPackets();
        } catch (Exception e) {
            e.printStackTrace();
        }

        setupKafka();

        String registrationMsg = String.format("{ \"id\": \"%s\", \"address\": \"localhost\", \"port\": \"%d\" }", id, port);
        sendToKafkaTopic("Register", registrationMsg);

        receiveCommands();
    }

    public void receiveCommands() {
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(record -> {
                String topic = record.topic();
                String message = record.value();

                if ("Command".equals(topic)) {
                    if ("Start".equals(message)) {
                        System.out.println("Starting Sensor Node...");
                        new Thread(this::handleSensorReadings).start();
                    } else if ("Stop".equals(message)) {
                        System.out.println("Stopping Sensor Node...");
                        running = false;
                        udpComm.close();
                        printReadingsAndExit();
                    }
                } else if ("Register".equals(topic)) {
                    try {
                        org.json.JSONObject json = new org.json.JSONObject(message);
                        String nodeId = json.getString("id");
                        String address = json.getString("address");
                        int nodePort = json.getInt("port");
                        if (!nodeId.equals(id)) {
                            registerNode(nodeId);
                            registeredNodes.put(nodeId, address + ":" + nodePort);
                            System.out.println("Registered node: " + nodeId + " -> " + address + ":" + nodePort);
                        }
                    } catch (Exception e) {
                        System.err.println("Failed to parse registration message: " + message);
                    }
                }
            });
        }
    }

    public void sendToKafkaTopic(String topic, String msg) {
        producer.send(new ProducerRecord<>(topic, msg));
    }

    private void handleSensorReadings() {
        while (running) {
            try {
                int index = (int) ((clock.currentTimeMillis() % 100) + 1);
                double reading = getSensorValueFromCSV(index);
                updateVectorTime();
                readings.add(new Reading(reading, clock.currentTimeMillis() ,vectorClock.clone()));
                sendUDPDataPacket(reading);
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private double getSensorValueFromCSV(int index) {
        String csvPath = "src/main/resources/readings.csv";
        try (Scanner scanner = new Scanner(new java.io.File(csvPath))) {
            int lineNum = 0;
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                lineNum++;
                if (lineNum == index + 1) {
                    String[] parts = line.split(",");
                    if (parts.length > 4) {
                        String value = parts[4].trim();
                        if (value.isEmpty()) {
                            return 0.0;
                        }
                        return Double.parseDouble(value);
                    } else {
                        return 0.0;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0.0;
    }

    private void sendUDPDataPacket(double reading) {
        String packet = String.format("{\"id\":\"%s\",\"reading\":%.2f,\"scalar\":%d,\"vector\":%s}",
                id, reading, clock.currentTimeMillis(), java.util.Arrays.toString(vectorClock));
        registeredNodes.forEach((nodeId, address) -> {
            try {
                String[] parts = address.split(":");
                String ip = parts[0];
                int nodePort = Integer.parseInt(parts[1]);
                udpComm.sendMessageWithRetransmission(packet, java.net.InetAddress.getByName(ip), nodePort);
            } catch (Exception e) {
                System.err.println("Failed to send packet to node " + nodeId + ": " + e.getMessage());
            }
        });
    }

    private void updateVectorTime() {
        int myIndex = nodeIndexMap.get(id);
        vectorClock[myIndex] += 1;
    }


    public void processReceivedPackets() {
        new Thread(() -> {
            while (running) {
                try {
                    DatagramPacket datagramPacket = udpComm.receivePacket();
                    if (datagramPacket == null) break;

                    String message = new String(datagramPacket.getData(), 0, datagramPacket.getLength());
                    if (message.startsWith("ACK:")) {
                        String ackMessage = message.substring(4);
                        udpComm.acknowledgments.add(Pair.of(ackMessage, datagramPacket.getPort()));
                    } else {
                        System.out.println("Data packet received: " + message);
                        udpComm.sendAcknowledgment(datagramPacket.getAddress(), datagramPacket.getPort(), message);

                        org.json.JSONObject json = new org.json.JSONObject(message);
                        String senderId = json.getString("id");
                        long receivedScalarTime = json.getLong("scalar");
                        double receivedReading = json.getDouble("reading");
                        long[] receivedVector = json.getJSONArray("vector").toList().stream()
                                .mapToLong(o -> ((Number) o).longValue())
                                .toArray();

                        String packetKey = senderId + "_" + receivedScalarTime + "_" + receivedReading;

                        if (receivedPackets.contains(packetKey)) {
                            System.out.println("Duplicate packet ignored: " + packetKey);
                            continue;
                        }

                        receivedPackets.add(packetKey);
                        if (receivedScalarTime > clock.currentTimeMillis()) {
                            clock.adjustClock(receivedScalarTime);
                        }
                        else if (receivedVector.length > vectorClock.length) {
                            long[] newVector = new long[receivedVector.length];
                            System.arraycopy(vectorClock, 0, newVector, 0, vectorClock.length);
                            vectorClock = newVector;
                        }

                        for (int i = 0; i < receivedVector.length; i++) {
                            vectorClock[i] = Math.max(vectorClock[i], receivedVector[i]);
                        }

                        synchronized (readings) {
                            readings.add(new Reading(receivedReading, clock.currentTimeMillis(), vectorClock.clone()));
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private void printReadingsAndExit() {
        printReadings();
        System.exit(0);
    }
    private void printReadings() {
        synchronized (readings) {
            if (readings.isEmpty()) {
                System.out.println("No readings available.");
                return;
            }

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                    .withZone(ZoneId.systemDefault());

            System.out.println("\n--- Readings sorted by Scalar Time ---");
            readings.stream()
                    .sorted((r1, r2) -> Long.compare(r1.scalar, r2.scalar))
                    .forEach(r -> System.out.println(
                            "Scalar time: " + formatter.format(Instant.ofEpochMilli(r.scalar)) +
                                    ", Vector time: " + java.util.Arrays.toString(r.vector) +
                                    ", Reading: " + r.value
                    ));

            double avgScalar = readings.stream()
                    .mapToDouble(r -> r.value)
                    .average()
                    .orElse(0.0);
            System.out.println("Average Reading (Scalar sort): " + avgScalar);

            System.out.println("\n--- Readings sorted by Vector Time ---");
            readings.stream()
                    .sorted((r1, r2) -> {
                        long[] v1 = r1.vector;
                        long[] v2 = r2.vector;
                        int len = Math.min(v1.length, v2.length);
                        for (int i = 0; i < len; i++) {
                            if (v1[i] != v2[i]) return Long.compare(v1[i], v2[i]);
                        }
                        return Integer.compare(v1.length, v2.length);
                    })
                    .forEach(r -> System.out.println(
                            "Vector time: " + java.util.Arrays.toString(r.vector) +
                                    ", Scalar time: " + formatter.format(Instant.ofEpochMilli(r.scalar)) +
                                    ", Reading: " + r.value
                    ));

            double avgVector = readings.stream()
                    .mapToDouble(r -> r.value)
                    .average()
                    .orElse(0.0);
            System.out.println("Average Reading (Vector sort): " + avgVector);
        }
    }


    private void registerNode(String nodeId) {
        if (!nodeIndexMap.containsKey(nodeId)) {
            int newIndex;
            try {
                newIndex = Integer.parseInt(nodeId);
            } catch (NumberFormatException e) {
                newIndex = nodeIndexMap.size();
            }
            nodeIndexMap.put(nodeId, newIndex);

            if (vectorClock.length <= newIndex) {
                long[] newVector = new long[newIndex + 1];
                System.arraycopy(vectorClock, 0, newVector, 0, vectorClock.length);
                vectorClock = newVector;
            }
        }
    }

    public static void main(String[] args) {
        String id = "1";
        int port = 5001;

        if (args.length > 0) {
            id = args[0];
        }
        if (args.length > 1) {
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.out.println("Invalid port, using default " + port);
            }
        }

        Logger kafkaLogger = (Logger) LoggerFactory.getLogger("org.apache.kafka");
        kafkaLogger.setLevel(Level.INFO);

        Sensor sensor = new Sensor();
        sensor.initialize(id, port);
    }
}


