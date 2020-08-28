package cj.netos.chasechain.bubbler;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

//每次冒泡时统计在该表
public class TrafficDashboardPointer {
    public static transient final String _COL_NAME = "traffic.dashboard.pointers";

    BigInteger itemCount;//物品总量，包含行为为0的所有物品；采用分阶段（lastBubbleTime）累加
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
        result.setInnateCommentRatio(innate.getCommentsRatio());
        result.setInnateLikeRatio(innate.getLikesRatio());
        result.setInnateRecommendsRatio(innate.getRecommendsRatio());

        ItemBehaviorPointer inner = innerBehaviorPointer;
        result.setInnerComments(inner.comments);
        result.setInnerLikes(inner.likes);
        result.setInnerRecommends(inner.recommends);
        result.setInnerCommentRatio(inner.getCommentsRatio());
        result.setInnerLikeRatio(inner.getLikesRatio());
        result.setInnerRecommendRatio(inner.getRecommendsRatio());
        return result;
    }
}
