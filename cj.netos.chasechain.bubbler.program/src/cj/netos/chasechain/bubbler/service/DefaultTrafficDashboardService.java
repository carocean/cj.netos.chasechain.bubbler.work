package cj.netos.chasechain.bubbler.service;

import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.cube.framework.IDocument;
import cj.lns.chip.sos.cube.framework.IQuery;
import cj.lns.chip.sos.cube.framework.TupleDocument;
import cj.netos.chasechain.bubbler.*;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.net.CircuitException;
import cj.ultimate.gson2.com.google.gson.Gson;
import com.mongodb.client.AggregateIterable;
import org.bson.Document;

import java.math.BigInteger;
import java.util.Arrays;

@CjService(name = "defaultTrafficDashboardService")
public class DefaultTrafficDashboardService extends AbstractService implements ITrafficDashboardService {

    @Override
    public TrafficDashboardPointer getPointer(String pool) throws CircuitException {
        ICube cube = cube(pool);
        //在etl.work项目中的AbstractService类中已建索引为倒序
        String cjql = String.format("select {'tuple':'*'}.sort({'tuple.lastBubbleTime':-1}).limit(1) from tuple %s %s where {}", TrafficDashboardPointer._COL_NAME, TrafficDashboardPointer.class.getName());
        IQuery<TrafficDashboardPointer> query = cube.createQuery(cjql);
        IDocument<TrafficDashboardPointer> document = query.getSingleResult();
        if (document == null) {
            TrafficDashboardPointer pointer = new TrafficDashboardPointer();
            pointer.setItemCount(new BigInteger("0"));
            ItemBehaviorPointer inner = new ItemBehaviorPointer();
            inner.setComments(new BigInteger("0"));
            inner.setLikes(new BigInteger("0"));
            inner.setRecommends(new BigInteger("0"));
            inner.setHasLikeItemCount(new BigInteger("0"));
            inner.setHasCommentItemCount(new BigInteger("0"));
            inner.setHasRecommendItemCount(new BigInteger("0"));

            ItemBehaviorPointer innate = new ItemBehaviorPointer();
            innate.setComments(new BigInteger("0"));
            innate.setLikes(new BigInteger("0"));
            innate.setRecommends(new BigInteger("0"));
            innate.setHasLikeItemCount(new BigInteger("0"));
            innate.setHasCommentItemCount(new BigInteger("0"));
            innate.setHasRecommendItemCount(new BigInteger("0"));

            pointer.setInnerBehaviorPointer(inner);
            pointer.setInnateBehaviorPointer(innate);
            pointer.setLastBubbleTime(0L);

            cube.saveDoc(TrafficDashboardPointer._COL_NAME, new TupleDocument<>(pointer));
            return pointer;
        }
        TrafficDashboardPointer pointer = document.tuple();

        ItemBehaviorPointer innate = pointer.getInnateBehaviorPointer();
        if (innate.getHasLikeItemCount() == null) {
            innate.setHasLikeItemCount(new BigInteger("0"));
        }
        if (innate.getHasCommentItemCount() == null) {
            innate.setHasCommentItemCount(new BigInteger("0"));
        }
        if (innate.getHasRecommendItemCount() == null) {
            innate.setHasRecommendItemCount(new BigInteger("0"));
        }

        ItemBehaviorPointer inner = pointer.getInnerBehaviorPointer();
        if (inner.getHasLikeItemCount() == null) {
            inner.setHasLikeItemCount(new BigInteger("0"));
        }
        if (inner.getHasCommentItemCount() == null) {
            inner.setHasCommentItemCount(new BigInteger("0"));
        }
        if (inner.getHasRecommendItemCount() == null) {
            inner.setHasRecommendItemCount(new BigInteger("0"));
        }
        return pointer;
    }

    @Override
    public void movePointer(String sourcePool, TrafficDashboardPointer fromPointer, long endTime) throws CircuitException {
        long beginTime = fromPointer.getLastBubbleTime();
        if (endTime < 1) {//如果为0表示当前时间
            endTime = System.currentTimeMillis();
        }
        if (beginTime == endTime) {//不动
            return;
        }
        ICube cube = cube(sourcePool);

        //统计一个时间窗口内的行为信息，并与仪表中的行为相加且更新
        //注意：必须有行为的物品才统计
        BigInteger itemCount = getItemCount(cube, beginTime, endTime);
        itemCount = itemCount.add(fromPointer.getItemCount());

        BigInteger[] innateLikeBehaviors = totalInnateLikeBehaviors(cube, beginTime, endTime);
        BigInteger[] innerLikeBehaviors = totalInnerLikeBehaviors(cube, beginTime, endTime);

        BigInteger[] innateCommentBehaviors = totalInnateCommentBehaviors(cube, beginTime, endTime);
        BigInteger[] innerCommentBehaviors = totalInnerCommentBehaviors(cube, beginTime, endTime);

        BigInteger[] innateRecommendBehaviors = totalInnateRecommendBehaviors(cube, beginTime, endTime);
        BigInteger[] innerRecommendBehaviors = totalInnerRecommendBehaviors(cube, beginTime, endTime);

        ItemBehaviorPointer innateBehaviorPointer = new ItemBehaviorPointer();
        innateBehaviorPointer.setHasLikeItemCount(innateLikeBehaviors[0]);
        innateBehaviorPointer.setLikes(innateLikeBehaviors[1]);

        innateBehaviorPointer.setHasCommentItemCount(innateCommentBehaviors[0]);
        innateBehaviorPointer.setComments(innateCommentBehaviors[1]);

        innateBehaviorPointer.setHasRecommendItemCount(innateRecommendBehaviors[0]);
        innateBehaviorPointer.setRecommends(innateRecommendBehaviors[1]);

        ItemBehaviorPointer innerBehaviorPointer = new ItemBehaviorPointer();
        innerBehaviorPointer.setHasLikeItemCount(innerLikeBehaviors[0]);
        innerBehaviorPointer.setLikes(innerLikeBehaviors[1]);

        innerBehaviorPointer.setHasCommentItemCount(innerCommentBehaviors[0]);
        innerBehaviorPointer.setComments(innerCommentBehaviors[1]);

        innerBehaviorPointer.setHasRecommendItemCount(innerRecommendBehaviors[0]);
        innerBehaviorPointer.setRecommends(innerRecommendBehaviors[1]);

        //阶段累加
        innateBehaviorPointer.addFrom(fromPointer.getInnateBehaviorPointer());
        innerBehaviorPointer.addFrom(fromPointer.getInnerBehaviorPointer());

        cube.updateDocOne(TrafficDashboardPointer._COL_NAME,
                Document.parse(String.format("{}")),
                Document.parse(String.format("{'$set':{'tuple.itemCount':%s,'tuple.innateBehaviorPointer':%s,'tuple.innerBehaviorPointer':%s,'tuple.lastBubbleTime':%s}}",
                        itemCount, new Gson().toJson(innateBehaviorPointer), new Gson().toJson(innerBehaviorPointer), endTime)));
    }

    private BigInteger[] totalInnateLikeBehaviors(ICube cube, long beginTime, long endTime) {
        AggregateIterable<Document> iterable = cube.aggregate(ItemBehavior._COL_NAME_INNATE, Arrays.asList(
                Document.parse(String.format("{'$match':{'tuple.likes':{'$gt':0},'$and':[{'tuple.utime':{'$gt':%s}},{'tuple.utime':{'$lte':%s}}]}}",
                        beginTime, endTime)),
                Document.parse(String.format("{'$group':{'_id':null,'likes':{'$sum':'$tuple.likes'},'itemCount':{'$sum':1}}}",
                        beginTime, endTime))
        ));
        Document total = null;
        for (Document document : iterable) {
            total = document;
            break;
        }
        BigInteger likes = null;
        BigInteger itemCount = null;
        if (total == null) {
            likes = new BigInteger("0");
            itemCount = new BigInteger("0");
        } else {
            likes = new BigInteger(total.get("likes") + "");
            itemCount = new BigInteger(total.get("itemCount") + "");
        }
        return new BigInteger[]{itemCount, likes};
    }

    private BigInteger[] totalInnerLikeBehaviors(ICube cube, long beginTime, long endTime) {
        AggregateIterable<Document> iterable = cube.aggregate(ItemBehavior._COL_NAME_INNER, Arrays.asList(
                Document.parse(String.format("{'$match':{'tuple.likes':{'$gt':0},'$and':[{'tuple.utime':{'$gt':%s}},{'tuple.utime':{'$lte':%s}}]}}",
                        beginTime, endTime)),
                Document.parse(String.format("{'$group':{'_id':null,'likes':{'$sum':'$tuple.likes'},'itemCount':{'$sum':1}}}",
                        beginTime, endTime))
        ));
        Document total = null;
        for (Document document : iterable) {
            total = document;
            break;
        }
        BigInteger likes = null;
        BigInteger itemCount = null;
        if (total == null) {
            likes = new BigInteger("0");
            itemCount = new BigInteger("0");
        } else {
            likes = new BigInteger(total.get("likes") + "");
            itemCount = new BigInteger(total.get("itemCount") + "");
        }
        return new BigInteger[]{itemCount, likes};
    }

    private BigInteger[] totalInnateCommentBehaviors(ICube cube, long beginTime, long endTime) {
        AggregateIterable<Document> iterable = cube.aggregate(ItemBehavior._COL_NAME_INNATE, Arrays.asList(
                Document.parse(String.format("{'$match':{'tuple.comments':{'$gt':0},'$and':[{'tuple.utime':{'$gt':%s}},{'tuple.utime':{'$lte':%s}}]}}",
                        beginTime, endTime)),
                Document.parse(String.format("{'$group':{'_id':null,'comments':{'$sum':'$tuple.comments'},'itemCount':{'$sum':1}}}",
                        beginTime, endTime))
        ));
        Document total = null;
        for (Document document : iterable) {
            total = document;
            break;
        }
        BigInteger comments = null;
        BigInteger itemCount = null;
        if (total == null) {
            comments = new BigInteger("0");
            itemCount = new BigInteger("0");
        } else {
            comments = new BigInteger(total.get("comments") + "");
            itemCount = new BigInteger(total.get("itemCount") + "");
        }
        return new BigInteger[]{itemCount, comments};
    }

    private BigInteger[] totalInnerCommentBehaviors(ICube cube, long beginTime, long endTime) {
        AggregateIterable<Document> iterable = cube.aggregate(ItemBehavior._COL_NAME_INNER, Arrays.asList(
                Document.parse(String.format("{'$match':{'tuple.comments':{'$gt':0},'$and':[{'tuple.utime':{'$gt':%s}},{'tuple.utime':{'$lte':%s}}]}}",
                        beginTime, endTime)),
                Document.parse(String.format("{'$group':{'_id':null,'comments':{'$sum':'$tuple.comments'},'itemCount':{'$sum':1}}}",
                        beginTime, endTime))
        ));
        Document total = null;
        for (Document document : iterable) {
            total = document;
            break;
        }
        BigInteger comments = null;
        BigInteger itemCount = null;
        if (total == null) {
            comments = new BigInteger("0");
            itemCount = new BigInteger("0");
        } else {
            comments = new BigInteger(total.get("comments") + "");
            itemCount = new BigInteger(total.get("itemCount") + "");
        }
        return new BigInteger[]{itemCount, comments};
    }

    private BigInteger[] totalInnateRecommendBehaviors(ICube cube, long beginTime, long endTime) {
        AggregateIterable<Document> iterable = cube.aggregate(ItemBehavior._COL_NAME_INNATE, Arrays.asList(
                Document.parse(String.format("{'$match':{'tuple.recommends':{'$gt':0},'$and':[{'tuple.utime':{'$gt':%s}},{'tuple.utime':{'$lte':%s}}]}}",
                        beginTime, endTime)),
                Document.parse(String.format("{'$group':{'_id':null,'recommends':{'$sum':'$tuple.recommends'},'itemCount':{'$sum':1}}}",
                        beginTime, endTime))
        ));
        Document total = null;
        for (Document document : iterable) {
            total = document;
            break;
        }
        BigInteger recommends = null;
        BigInteger itemCount = null;
        if (total == null) {
            recommends = new BigInteger("0");
            itemCount = new BigInteger("0");
        } else {
            recommends = new BigInteger(total.get("recommends") + "");
            itemCount = new BigInteger(total.get("itemCount") + "");
        }
        return new BigInteger[]{itemCount, recommends};
    }

    private BigInteger[] totalInnerRecommendBehaviors(ICube cube, long beginTime, long endTime) {
        AggregateIterable<Document> iterable = cube.aggregate(ItemBehavior._COL_NAME_INNER, Arrays.asList(
                Document.parse(String.format("{'$match':{'tuple.recommends':{'$gt':0},'$and':[{'tuple.utime':{'$gt':%s}},{'tuple.utime':{'$lte':%s}}]}}",
                        beginTime, endTime)),
                Document.parse(String.format("{'$group':{'_id':null,'recommends':{'$sum':'$tuple.recommends'},'itemCount':{'$sum':1}}}",
                        beginTime, endTime))
        ));
        Document total = null;
        for (Document document : iterable) {
            total = document;
            break;
        }
        BigInteger recommends = null;
        BigInteger itemCount = null;
        if (total == null) {
            recommends = new BigInteger("0");
            itemCount = new BigInteger("0");
        } else {
            recommends = new BigInteger(total.get("recommends") + "");
            itemCount = new BigInteger(total.get("itemCount") + "");
        }
        return new BigInteger[]{itemCount, recommends};
    }

    private BigInteger getItemCount(ICube cube, long beginTime, long endTime) {
        long count = cube.tupleCount(ContentItem._COL_NAME, String.format("{'$and':[{'tuple.ctime':{'$gt':%s}},{'tuple.ctime':{'$lte':%s}}]}",
                beginTime, endTime));
        return new BigInteger(count + "");
    }

    private ItemBehaviorPointer totalInnateBehaviors(ICube cube, long beginTime, long endTime) {
        AggregateIterable<Document> iterable = cube.aggregate(ItemBehavior._COL_NAME_INNATE, Arrays.asList(
                Document.parse(String.format("{'$match':{'$and':[{'tuple.utime':{'$gt':%s}},{'tuple.utime':{'$lte':%s}}]}}",
                        beginTime, endTime)),
                Document.parse(String.format("{'$group':{'_id':null,'likes':{'$sum':'$tuple.likes'},'comments':{'$sum':'$tuple.comments'},'recommends':{'$sum':'$tuple.recommends'}}}",
                        beginTime, endTime))
        ));
        Document total = null;
        for (Document document : iterable) {
            total = document;
            break;
        }
        BigInteger likes = null;
        BigInteger comments = null;
        BigInteger recommends = null;
        if (total == null) {
            likes = new BigInteger("0");
            comments = new BigInteger("0");
            recommends = new BigInteger("0");
        } else {
            likes = new BigInteger(total.get("likes") + "");
            comments = new BigInteger(total.get("comments") + "");
            recommends = new BigInteger(total.get("recommends") + "");
        }

        ItemBehaviorPointer behaviorPointer = new ItemBehaviorPointer();
        behaviorPointer.setComments(comments);
        behaviorPointer.setLikes(likes);
        behaviorPointer.setRecommends(recommends);
        return behaviorPointer;
    }

    private ItemBehaviorPointer totalInnerBehaviors(ICube cube, long beginTime, long endTime) {
        AggregateIterable<Document> iterable = cube.aggregate(ItemBehavior._COL_NAME_INNER, Arrays.asList(
                Document.parse(String.format("{'$match':{'$and':[{'tuple.utime':{'$gt':%s}},{'tuple.utime':{'$lte':%s}}]}}",
                        beginTime, endTime)),
                Document.parse(String.format("{'$group':{'_id':null,'likes':{'$sum':'$tuple.likes'},'comments':{'$sum':'$tuple.comments'},'recommends':{'$sum':'$tuple.recommends'}}}",
                        beginTime, endTime))
        ));
        Document total = null;
        for (Document document : iterable) {
            total = document;
            break;
        }
        BigInteger likes = null;
        BigInteger comments = null;
        BigInteger recommends = null;
        if (total == null) {
            likes = new BigInteger("0");
            comments = new BigInteger("0");
            recommends = new BigInteger("0");
        } else {
            likes = new BigInteger(total.get("likes") + "");
            comments = new BigInteger(total.get("comments") + "");
            recommends = new BigInteger(total.get("recommends") + "");
        }

        ItemBehaviorPointer behaviorPointer = new ItemBehaviorPointer();
        behaviorPointer.setComments(comments);
        behaviorPointer.setLikes(likes);
        behaviorPointer.setRecommends(recommends);
        return behaviorPointer;
    }
}
