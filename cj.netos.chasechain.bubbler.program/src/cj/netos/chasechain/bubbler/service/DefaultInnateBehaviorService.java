package cj.netos.chasechain.bubbler.service;

import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.cube.framework.IDocument;
import cj.lns.chip.sos.cube.framework.IQuery;
import cj.lns.chip.sos.cube.framework.TupleDocument;
import cj.netos.chasechain.bubbler.*;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.studio.ecm.net.CircuitException;
import org.bson.Document;
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

@CjService(name = "defaultInnateBehaviorService")
public class DefaultInnateBehaviorService extends AbstractService implements IInnateBehaviorService {
    @CjServiceRef(refByName = "@.redis.cluster")
    JedisCluster jedisCluster;
    @CjServiceRef(refByName = "defaultContentItemService")
    IContentItemService contentItemService;

    @Override
    public void putRedisItems(String pool, TrafficDashboardPointer trafficDashBoardPointer) throws CircuitException {
        int limit = 100;
        int offset = 0;
        long lastBubbleTime = trafficDashBoardPointer.getLastBubbleTime();
        BigInteger itemCount = trafficDashBoardPointer.getItemCount();
        if (itemCount.compareTo(new BigInteger("0")) == 0) {
            long count = contentItemService.totalCount(pool, lastBubbleTime);
            count = count == 0 ? 1 : count;
            itemCount = new BigInteger(count + "");
        }
        BigDecimal bigItemCount = (new BigDecimal(itemCount));
        ItemBehaviorPointer innateBehavior = trafficDashBoardPointer.getInnateBehaviorPointer();
        BigDecimal innateLikesRatio = null;
        BigDecimal innateCommentsRatio = null;
        BigDecimal innateRecommendsRatio = null;
        if (itemCount.compareTo(new BigInteger("0")) == 0) {
            innateLikesRatio = new BigDecimal("0");
            innateCommentsRatio = new BigDecimal("0");
            innateRecommendsRatio = new BigDecimal("0");
        } else {
            innateLikesRatio = new BigDecimal(innateBehavior.getLikes()).divide(bigItemCount, 14, RoundingMode.DOWN);
            innateCommentsRatio = new BigDecimal(innateBehavior.getComments()).divide(bigItemCount, 14, RoundingMode.DOWN);
            innateRecommendsRatio = new BigDecimal(innateBehavior.getRecommends()).divide(bigItemCount, 14, RoundingMode.DOWN);
        }
        while (true) {
            List<ItemBehavior> behaviors = pageBehavior(pool, lastBubbleTime, limit, offset);
            if (behaviors.isEmpty()) {
                break;
            }
            offset += behaviors.size();
            for (ItemBehavior behavior : behaviors) {
                BigDecimal argLikes = new BigDecimal(behavior.getLikes()).divide(bigItemCount, 14, RoundingMode.DOWN);
                BigDecimal argComments = new BigDecimal(behavior.getComments()).divide(bigItemCount, 14, RoundingMode.DOWN);
                BigDecimal argRecommends = new BigDecimal(behavior.getRecommends()).divide(bigItemCount, 14, RoundingMode.DOWN);
                if (argLikes.compareTo(innateLikesRatio) <= 0
                        && argComments.compareTo(innateCommentsRatio) <= 0
                        && argRecommends.compareTo(innateRecommendsRatio) <= 0) {
                    continue;
                }
                //将itemid添加到redis集合中
                jedisCluster.sadd(Constants.set_bubbler_items_redis_key, behavior.getItem());
            }
        }
    }


    private List<ItemBehavior> pageBehavior(String pool, long beginTIme, int limit, int offset) throws CircuitException {
        ICube cube = cube(pool);
        String cjql = String.format("select {'tuple':'*'}.limit(%s).skip(%s) from tuple %s %s where {'tuple.utime':{'$gt':%s}}",
                limit, offset, ItemBehavior._COL_NAME_INNATE, ItemBehavior.class.getName(), beginTIme);
        IQuery<ItemBehavior> query = cube.createQuery(cjql);
        List<IDocument<ItemBehavior>> list = query.getResultList();
        List<ItemBehavior> itemBehaviors = new ArrayList<>();
        for (IDocument<ItemBehavior> document : list) {
            itemBehaviors.add(document.tuple());
        }
        return itemBehaviors;
    }

    @Override
    public ItemBehavior getBehavior(String pool, String item) throws CircuitException {
        ICube cube = cube(pool);
        String cjql = String.format("select {'tuple':'*'}.limit(1) from tuple %s %s where {'tuple.id':'%s'}",
                ItemBehavior._COL_NAME_INNATE, ItemBehavior.class.getName(), item);
        IQuery<ItemBehavior> query = cube.createQuery(cjql);
        IDocument<ItemBehavior> document = query.getSingleResult();
        if (document == null) {
            ItemBehavior behavior = new ItemBehavior();
            behavior.setRecommends(0L);
            behavior.setComments(0L);
            behavior.setLikes(0L);
            behavior.setItem(item);
            behavior.setUtime(0L);//0表示不存在
            return behavior;
        }
        return document.tuple();
    }

    @Override
    public void addBehavior(String pool, ItemBehavior innateBehavior) throws CircuitException {
        ICube cube = cube(pool);
        cube.saveDoc(ItemBehavior._COL_NAME_INNATE, new TupleDocument<>(innateBehavior));
    }

    @Override
    public void upateBehavior(String pool, ItemBehavior innateBehavior) throws CircuitException {
        ICube cube = cube(pool);
        cube.updateDocOne(ItemBehavior._COL_NAME_INNATE,
                Document.parse(String.format("{'tuple.item':'%s'}", innateBehavior.getItem())),
                Document.parse(String.format("{'$set':{'tuple.comments':%s,'tuple.likes':%s,'tuple.recommends':%s,'tuple.utime':%s}}",
                        innateBehavior.getComments(), innateBehavior.getLikes(), innateBehavior.getRecommends(), System.currentTimeMillis()))
        );
    }
}
