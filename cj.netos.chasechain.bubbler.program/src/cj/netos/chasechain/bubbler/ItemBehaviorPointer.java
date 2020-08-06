package cj.netos.chasechain.bubbler;

import java.math.BigInteger;

public class ItemBehaviorPointer {
    BigInteger likes;
    BigInteger comments;
    BigInteger recommends;

    public BigInteger getLikes() {
        return likes;
    }

    public void setLikes(BigInteger likes) {
        this.likes = likes;
    }

    public BigInteger getComments() {
        return comments;
    }

    public void setComments(BigInteger comments) {
        this.comments = comments;
    }

    public BigInteger getRecommends() {
        return recommends;
    }

    public void setRecommends(BigInteger recommends) {
        this.recommends = recommends;
    }
}
