package cj.netos.chasechain.bubbler;

/**
 * 流量池
 */
public class TrafficPool {
    public final static transient String _COL_NAME = "chasechain.traffic.pools";
    String id;
    String title;
    String icon;
    String geoCode;//地理区域代码，如国、省、市、县区等地理代码
    String geoTitle;//地理区域名
    boolean isGeosphere;//是否是地理流量池，如果是则geoCode必不能为空
    int state;//0为正常；-1为停止
    int level;//0全国；1省级；2市级；3区县级；4自定义
    int index;//只有非地理流量池才有效，表示非地理流量池的的当前索引号，-1表示地理流量池
    String parent;//归属的上级流量池
    long ctime;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public boolean isGeosphere() {
        return isGeosphere;
    }

    public void setGeosphere(boolean geosphere) {
        isGeosphere = geosphere;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public String getTrafficPoolCube() {
        return String.format("%s.%s.%s", _COL_NAME, level, id);
    }

    public static String getColName() {
        return _COL_NAME;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getIcon() {
        return icon;
    }

    public void setIcon(String icon) {
        this.icon = icon;
    }

    public String getGeoCode() {
        return geoCode;
    }

    public void setGeoCode(String geoCode) {
        this.geoCode = geoCode;
    }

    public String getGeoTitle() {
        return geoTitle;
    }

    public void setGeoTitle(String geoTitle) {
        this.geoTitle = geoTitle;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public long getCtime() {
        return ctime;
    }

    public void setCtime(long ctime) {
        this.ctime = ctime;
    }
}
