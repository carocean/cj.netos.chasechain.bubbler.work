package cj.netos.chasechain.bubbler.service;

import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.cube.framework.IDocument;
import cj.lns.chip.sos.cube.framework.IQuery;
import cj.lns.chip.sos.cube.framework.TupleDocument;
import cj.netos.chasechain.bubbler.*;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.net.CircuitException;
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
            ItemBehaviorPointer innate = new ItemBehaviorPointer();
            innate.setComments(new BigInteger("0"));
            innate.setLikes(new BigInteger("0"));
            pointer.setInnerBehaviorPointer(inner);
            pointer.setInnateBehaviorPointer(innate);
            pointer.setLastBubbleTime(0L);
            return pointer;
        }
        return document.tuple();
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
        TrafficDashboardPointer pointer = new TrafficDashboardPointer();

        BigInteger itemCount = getItemCount(cube, beginTime, endTime);
        ItemBehaviorPointer innateBehaviorPointer = totalInnateBehaviors(cube, beginTime, endTime);
        ItemBehaviorPointer innerBehaviorPointer = totalInnerBehaviors(cube, beginTime, endTime);

        pointer.setLastBubbleTime(endTime);
        pointer.setItemCount(itemCount);
        pointer.setInnerBehaviorPointer(innerBehaviorPointer);
        pointer.setInnateBehaviorPointer(innateBehaviorPointer);

        cube.saveDoc(TrafficDashboardPointer._COL_NAME, new TupleDocument<>(pointer));
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
        Document total=null;
        for (Document document : iterable) {
            total=document;
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
        Document total=null;
        for (Document document : iterable) {
            total=document;
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

    @Override
    public void clearPointersExceptTop(String pool,int retains) throws CircuitException {
        ICube cube = cube(pool);
        //在etl.work项目中的AbstractService类中已建索引为倒序
        String cjql = String.format("select {'tuple':'*'}.sort({'tuple.lastBubbleTime':-1}).limit(1).skip(%s) from tuple %s %s where {}",retains-1, TrafficDashboardPointer._COL_NAME, TrafficDashboardPointer.class.getName());
        IQuery<TrafficDashboardPointer> query = cube.createQuery(cjql);
        IDocument<TrafficDashboardPointer> document=query.getSingleResult();
        if (document == null) {
            return;
        }
        TrafficDashboardPointer pointer=document.tuple();
        cube.deleteDocs(TrafficDashboardPointer._COL_NAME, String.format("{'tuple.lastBubbleTime':{'$lt':%s}}",pointer.getLastBubbleTime()));
    }
}
