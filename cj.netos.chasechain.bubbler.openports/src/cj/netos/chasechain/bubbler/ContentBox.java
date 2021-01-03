package cj.netos.chasechain.bubbler;

/**
 * 内容盒子<br>如网流管道、地圈的地理感知器
 * <br>存入TrafficPoolCube
 */
public class ContentBox {
    public final static transient String _COL_NAME = "content.boxes";
    String id;//内容盒的标识由于是pointer的类型+标识的md5生成的，所以在所有流量池中都唯一
    BoxPointer pointer;
    LatLng location;//盒子可能有位置属性，如地理感知器
    String upstreamPool;//来自上游的流量池
    boolean isBubbled;//是否已冒泡了
    long ctime;
    int isChanged;

    public int getIsChanged() {
        return isChanged;
    }

    public void setIsChanged(int isChanged) {
        this.isChanged = isChanged;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean isBubbled() {
        return isBubbled;
    }

    public void setBubbled(boolean bubbled) {
        isBubbled = bubbled;
    }

    public BoxPointer getPointer() {
        return pointer;
    }

    public void setPointer(BoxPointer pointer) {
        this.pointer = pointer;
    }

    public LatLng getLocation() {
        return location;
    }

    public void setLocation(LatLng location) {
        this.location = location;
    }

    public String getUpstreamPool() {
        return upstreamPool;
    }

    public void setUpstreamPool(String upstreamPool) {
        this.upstreamPool = upstreamPool;
    }

    public long getCtime() {
        return ctime;
    }

    public void setCtime(long ctime) {
        this.ctime = ctime;
    }
}
