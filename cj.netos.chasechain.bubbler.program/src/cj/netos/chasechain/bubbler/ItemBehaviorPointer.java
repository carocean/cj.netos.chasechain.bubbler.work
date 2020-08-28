package cj.netos.chasechain.bubbler;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

public class ItemBehaviorPointer {
    BigInteger hasLikeItemCount;//具有赞的物品总数
    BigInteger hasCommentItemCount;//具有评的物品总数
    BigInteger hasRecommendItemCount;//具有荐的物品总数

    BigInteger likes;
    BigInteger comments;
    BigInteger recommends;

    public BigDecimal getLikesRatio() {
        if (new BigInteger("0").compareTo(hasLikeItemCount) >= 0) {
            return new BigDecimal("0");
        }
        return new BigDecimal(likes).divide(new BigDecimal(hasLikeItemCount), 14, RoundingMode.DOWN);
    }

    public BigDecimal getCommentsRatio() {
        if (new BigInteger("0").compareTo(hasCommentItemCount) >= 0) {
            return new BigDecimal("0");
        }
        return new BigDecimal(comments).divide(new BigDecimal(hasCommentItemCount), 14, RoundingMode.DOWN);
    }

    public BigDecimal getRecommendsRatio() {
        if (new BigInteger("0").compareTo(hasRecommendItemCount) >= 0) {
            return new BigDecimal("0");
        }
        return new BigDecimal(recommends).divide(new BigDecimal(hasRecommendItemCount), 14, RoundingMode.DOWN);
    }

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

    public BigInteger getHasLikeItemCount() {
        return hasLikeItemCount;
    }

    public void setHasLikeItemCount(BigInteger hasLikeItemCount) {
        this.hasLikeItemCount = hasLikeItemCount;
    }

    public BigInteger getHasCommentItemCount() {
        return hasCommentItemCount;
    }

    public void setHasCommentItemCount(BigInteger hasCommentItemCount) {
        this.hasCommentItemCount = hasCommentItemCount;
    }

    public BigInteger getHasRecommendItemCount() {
        return hasRecommendItemCount;
    }

    public void setHasRecommendItemCount(BigInteger hasRecommendItemCount) {
        this.hasRecommendItemCount = hasRecommendItemCount;
    }

    public void addFrom(ItemBehaviorPointer p) {
        hasLikeItemCount = hasLikeItemCount == null ? new BigInteger("0") : hasLikeItemCount;
        hasCommentItemCount = hasCommentItemCount == null ? new BigInteger("0") : hasCommentItemCount;
        hasRecommendItemCount = hasRecommendItemCount == null ? new BigInteger("0") : hasRecommendItemCount;
        if (p.hasLikeItemCount != null) {
            hasLikeItemCount = hasLikeItemCount.add(p.hasLikeItemCount);
        }
        if (p.hasCommentItemCount != null) {
            hasCommentItemCount = hasCommentItemCount.add(p.hasCommentItemCount);
        }
        if (p.hasRecommendItemCount != null) {
            hasRecommendItemCount = hasRecommendItemCount.add(p.hasRecommendItemCount);
        }

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
