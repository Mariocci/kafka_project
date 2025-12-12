package sensorNode;

class Reading {
    double value;
    long scalar;
    long[] vector;

    Reading(double value, long scalar, long[] vector) {
        this.value = value;
        this.scalar = scalar;
        this.vector = vector.clone();
    }
}