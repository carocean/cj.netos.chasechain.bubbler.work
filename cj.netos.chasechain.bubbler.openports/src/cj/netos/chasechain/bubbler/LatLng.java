package cj.netos.chasechain.bubbler;

public class LatLng {
    double longitude;
    double latitude;

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public String toText() {
        return String.format("%s,%s", longitude, latitude);
    }
}
