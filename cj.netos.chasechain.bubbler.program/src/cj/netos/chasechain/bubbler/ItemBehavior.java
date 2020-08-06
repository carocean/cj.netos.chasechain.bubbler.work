package cj.netos.chasechain.bubbler;

public class ItemBehavior {
    public static transient final String _COL_NAME_INNATE = "behavior.innate";
    public static transient final String _COL_NAME_INNER = "behavior.inner";
    String item;
    long comments;
    long likes;
    long recommends;
    long ctime;

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public long getComments() {
        return comments;
    }

    public void setComments(long comments) {
        this.comments = comments;
    }

    public long getLikes() {
        return likes;
    }

    public void setLikes(long likes) {
        this.likes = likes;
    }

    public long getRecommends() {
        return recommends;
    }

    public void setRecommends(long recommends) {
        this.recommends = recommends;
    }

    public long getCtime() {
        return ctime;
    }

    public void setCtime(long ctime) {
        this.ctime = ctime;
    }
}
