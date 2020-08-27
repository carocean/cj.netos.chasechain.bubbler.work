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

    public void addFrom(ItemBehaviorPointer p) {
        likes = likes == null ? new BigInteger("0") : likes;
        comments = comments == null ? new BigInteger("0") : comments;
        recommends = recommends == null ? new BigInteger("0") : recommends;

        if (p.likes != null) {
            likes = likes.add(p.likes);
        }
        if (p.comments != null) {
            comments = comments.add(p.comments);
        }
        if (p.recommends != null) {
            recommends = recommends.add(p.recommends);
        }
    }
}
