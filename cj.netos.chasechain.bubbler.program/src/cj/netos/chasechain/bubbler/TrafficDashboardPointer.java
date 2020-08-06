package cj.netos.chasechain.bubbler;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

//每次冒泡时统计在该表
public class TrafficDashboardPointer {
    public static transient final String _COL_NAME = "traffic.dashboard.pointers";

    BigInteger itemCount;//截止到lastBubbleTime时间池中内容物数量
    ItemBehaviorPointer innateBehaviorPointer;
    ItemBehaviorPointer innerBehaviorPointer;
    long lastBubbleTime;//记录上次冒泡取抽时间,该字段不要改名，见：etl.work项目的AbstractService类

    public BigInteger getItemCount() {
        return itemCount;
    }

    public void setItemCount(BigInteger itemCount) {
        this.itemCount = itemCount;
    }

    public ItemBehaviorPointer getInnateBehaviorPointer() {
        return innateBehaviorPointer;
    }

    public void setInnateBehaviorPointer(ItemBehaviorPointer innateBehaviorPointer) {
        this.innateBehaviorPointer = innateBehaviorPointer;
    }

    public ItemBehaviorPointer getInnerBehaviorPointer() {
        return innerBehaviorPointer;
    }

    public void setInnerBehaviorPointer(ItemBehaviorPointer innerBehaviorPointer) {
        this.innerBehaviorPointer = innerBehaviorPointer;
    }

    public long getLastBubbleTime() {
        return lastBubbleTime;
    }

    public void setLastBubbleTime(long lastBubbleTime) {
        this.lastBubbleTime = lastBubbleTime;
    }

    public TrafficeBashboradResult copyTo(String poolId) {
        TrafficeBashboradResult result = new TrafficeBashboradResult();
        result.setPool(poolId);
        result.setItemCount(itemCount);
        result.setLastBubbleTime(lastBubbleTime);
        ItemBehaviorPointer innate = innateBehaviorPointer;
        result.setInnateComments(innate.comments);
        result.setInnateLikes(innate.likes);
        result.setInnateRecommends(innate.recommends);
        if (itemCount.compareTo(new BigInteger("0"))!=0) {
            result.setInnateCommentRatio(new BigDecimal(innate.comments + "").divide(new BigDecimal(itemCount + ""), 14, RoundingMode.DOWN));
            result.setInnateLikeRatio(new BigDecimal(innate.likes + "").divide(new BigDecimal(itemCount + ""), 14, RoundingMode.DOWN));
            result.setInnateRecommendsRatio(new BigDecimal(innate.recommends + "").divide(new BigDecimal(itemCount + ""), 14, RoundingMode.DOWN));
        }else{
            result.setInnateCommentRatio(new BigDecimal("0.00"));
            result.setInnateLikeRatio(new BigDecimal("0.00"));
            result.setInnateRecommendsRatio(new BigDecimal("0.00"));
        }

        ItemBehaviorPointer inner = innerBehaviorPointer;
        result.setInnerComments(inner.comments);
        result.setInnerLikes(inner.likes);
        result.setInnerRecommends(inner.recommends);
        if (itemCount.compareTo(new BigInteger("0"))!=0) {
            result.setInnerCommentRatio(new BigDecimal(inner.comments + "").divide(new BigDecimal(itemCount + ""), 14, RoundingMode.DOWN));
            result.setInnerLikeRatio(new BigDecimal(inner.likes + "").divide(new BigDecimal(itemCount + ""), 14, RoundingMode.DOWN));
            result.setInnerRecommendRatio(new BigDecimal(inner.recommends + "").divide(new BigDecimal(itemCount + ""), 14, RoundingMode.DOWN));
        }else{
            result.setInnerCommentRatio(new BigDecimal("0.00"));
            result.setInnerLikeRatio(new BigDecimal("0.00"));
            result.setInnerRecommendRatio(new BigDecimal("0.00"));
        }
        return result;
    }
}
