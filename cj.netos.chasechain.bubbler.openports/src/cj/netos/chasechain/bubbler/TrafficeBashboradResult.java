package cj.netos.chasechain.bubbler;

import java.math.BigDecimal;
import java.math.BigInteger;

public class TrafficeBashboradResult {
    String pool;
    BigInteger itemCount;//截止到lastBubbleTime时间池中内容物数量
    long lastBubbleTime;//记录上次冒泡取抽时间,该字段不要改名，见：etl.work项目的AbstractService类
    BigInteger innateLikes;
    BigDecimal innateLikeRatio;
    BigInteger innateComments;
    BigDecimal innateCommentRatio;
    BigInteger innateRecommends;
    BigDecimal innateRecommendsRatio;
    BigInteger innerLikes;
    BigDecimal innerLikeRatio;
    BigInteger innerComments;
    BigDecimal innerCommentRatio;
    BigInteger innerRecommends;
    BigDecimal innerRecommendRatio;

    public String getPool() {
        return pool;
    }

    public void setPool(String pool) {
        this.pool = pool;
    }

    public BigInteger getItemCount() {
        return itemCount;
    }

    public void setItemCount(BigInteger itemCount) {
        this.itemCount = itemCount;
    }

    public long getLastBubbleTime() {
        return lastBubbleTime;
    }

    public void setLastBubbleTime(long lastBubbleTime) {
        this.lastBubbleTime = lastBubbleTime;
    }

    public BigInteger getInnateLikes() {
        return innateLikes;
    }

    public void setInnateLikes(BigInteger innateLikes) {
        this.innateLikes = innateLikes;
    }

    public BigDecimal getInnateLikeRatio() {
        return innateLikeRatio;
    }

    public void setInnateLikeRatio(BigDecimal innateLikeRatio) {
        this.innateLikeRatio = innateLikeRatio;
    }

    public BigInteger getInnateComments() {
        return innateComments;
    }

    public void setInnateComments(BigInteger innateComments) {
        this.innateComments = innateComments;
    }

    public BigDecimal getInnateCommentRatio() {
        return innateCommentRatio;
    }

    public void setInnateCommentRatio(BigDecimal innateCommentRatio) {
        this.innateCommentRatio = innateCommentRatio;
    }

    public BigInteger getInnateRecommends() {
        return innateRecommends;
    }

    public void setInnateRecommends(BigInteger innateRecommends) {
        this.innateRecommends = innateRecommends;
    }

    public BigDecimal getInnateRecommendsRatio() {
        return innateRecommendsRatio;
    }

    public void setInnateRecommendsRatio(BigDecimal innateRecommendsRatio) {
        this.innateRecommendsRatio = innateRecommendsRatio;
    }

    public BigInteger getInnerLikes() {
        return innerLikes;
    }

    public void setInnerLikes(BigInteger innerLikes) {
        this.innerLikes = innerLikes;
    }

    public BigDecimal getInnerLikeRatio() {
        return innerLikeRatio;
    }

    public void setInnerLikeRatio(BigDecimal innerLikeRatio) {
        this.innerLikeRatio = innerLikeRatio;
    }

    public BigInteger getInnerComments() {
        return innerComments;
    }

    public void setInnerComments(BigInteger innerComments) {
        this.innerComments = innerComments;
    }

    public BigDecimal getInnerCommentRatio() {
        return innerCommentRatio;
    }

    public void setInnerCommentRatio(BigDecimal innerCommentRatio) {
        this.innerCommentRatio = innerCommentRatio;
    }

    public BigInteger getInnerRecommends() {
        return innerRecommends;
    }

    public void setInnerRecommends(BigInteger innerRecommends) {
        this.innerRecommends = innerRecommends;
    }

    public BigDecimal getInnerRecommendRatio() {
        return innerRecommendRatio;
    }

    public void setInnerRecommendRatio(BigDecimal innerRecommendRatio) {
        this.innerRecommendRatio = innerRecommendRatio;
    }
}
