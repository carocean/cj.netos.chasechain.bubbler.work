package cj.netos.chasechain.bubbler;

/**
 * 流量池分配的固定用户<br>
 * 如果即限定了用户，且geoCode为非空，则表示除了geoCode之内的用户和限定用户之外的用户是无权拉取内容的<br>
 * 未来可用此表作为活跃用户排行榜
 * <br>存入TrafficPoolCube
 */
public class TrafficPerson {
    public final static transient String _COL_NAME = "persons";
    String person;
    long ctime;
    long liveness;//活跃度，该字保留

    public String getPerson() {
        return person;
    }

    public void setPerson(String person) {
        this.person = person;
    }

    public long getCtime() {
        return ctime;
    }

    public void setCtime(long ctime) {
        this.ctime = ctime;
    }

    public long getLiveness() {
        return liveness;
    }

    public void setLiveness(long liveness) {
        this.liveness = liveness;
    }
}
