package cj.netos.chasechain.bubbler;


/**
 * 内容物<br>
 * <br>存入TrafficPoolCube
 */
public class ContentItem {
    public final static transient String _COL_NAME = "content.items";
    String id;//标识来自由pointer的类型+标识的md5，所以在所有流量池中都是唯一的，只要告诉内容物在哪个池，就可以在池中找到它
    ItemPointer pointer;
    String box;//归属的内容盒
    LatLng location;//内容物可能有位置属性
    String upstreamPool;//来自上游的流量池，一般是低级池
    boolean isBubbled;//是否已冒泡了
    long ctime;

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

    public ItemPointer getPointer() {
        return pointer;
    }

    public void setPointer(ItemPointer pointer) {
        this.pointer = pointer;
    }

    public String getBox() {
        return box;
    }

    public void setBox(String box) {
        this.box = box;
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
