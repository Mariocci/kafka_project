package sensorNode;

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class UDPCommunication {
    private SimpleSimulatedDatagramSocket socket;
    private InetAddress localHost;
    private final int port;
    private final ConcurrentHashMap<String, String> receivedData = new ConcurrentHashMap<>();
    final Set<Pair<String, Integer>> acknowledgments = ConcurrentHashMap.newKeySet();

    public UDPCommunication(int port, double lossRate, int averageDelay) throws SocketException {
        this.port = port;
        try {
            socket = new SimpleSimulatedDatagramSocket(port, lossRate, averageDelay); // custom simulated socket
            localHost = InetAddress.getByName("localhost");
        } catch (UnknownHostException e) {
            throw new RuntimeException("Could not resolve local host", e);
        }
    }


    public void sendAcknowledgment(InetAddress address, int port, String message) {
        String ack = "ACK:" + message;
        DatagramPacket ackPacket = new DatagramPacket(ack.getBytes(), ack.length(), address, port);
        try {
            socket.send(ackPacket);
        } catch (IOException e) {
            System.err.println("Failed to send acknowledgment: " + ack);
        }
    }

    public void sendMessageWithRetransmission(String message, InetAddress address, int port) throws InterruptedException {
        int retryCount = 5;
        while (!acknowledgments.contains(Pair.of(message,port)) && retryCount > 0) {
            try {
                DatagramPacket packet = new DatagramPacket(message.getBytes(), message.length(), address, port);
                System.out.println("Sending message: " + message);
                socket.send(packet);
                TimeUnit.MILLISECONDS.sleep(1000);
                retryCount--;
            } catch (IOException e) {
                System.err.println("Error while sending message: " + message);
            }
        }

        if (!acknowledgments.contains(Pair.of(message, port))) {
            System.out.println("Failed to send message after retries: " + message);
        } else {
            System.out.println("Acknowledgment received for message: " + message);
        }
    }
    public void close() {
        socket.close();
    }

    public DatagramPacket receivePacket() {
        try {
            byte[] buffer = new byte[1024];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            socket.receive(packet);
            return packet;
        } catch (SocketException e) {
            return null;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
