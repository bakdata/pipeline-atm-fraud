package com.bakdata.kafka;

import lombok.experimental.UtilityClass;

/**
 * From <a href="https://www.baeldung.com/java-find-distance-between-points#calculate-the-distance-using-the-haversine
 * -formula">Baeldung</a>
 */
@UtilityClass
class DistanceUtil {
    private static final int EARTH_RADIUS = 6371;

    static double calculateDistance(final Location start, final Location end) {
        double startLat = start.getLatitude();
        final double startLong = start.getLongitude();
        double endLat = end.getLatitude();
        final double endLong = end.getLongitude();

        final double dLat = Math.toRadians((endLat - startLat));
        final double dLong = Math.toRadians((endLong - startLong));

        startLat = Math.toRadians(startLat);
        endLat = Math.toRadians(endLat);

        final double a = haversine(dLat) + Math.cos(startLat) * Math.cos(endLat) * haversine(dLong);
        final double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return EARTH_RADIUS * c;
    }

    private static double haversine(final double val) {
        return Math.pow(Math.sin(val / 2), 2);
    }
}
