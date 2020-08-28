package cj.netos.chasechain.bubbler.service;

import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.cube.framework.IDocument;
import cj.lns.chip.sos.cube.framework.IQuery;
import cj.netos.chasechain.bubbler.*;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.studio.ecm.net.CircuitException;
import redis.clients.jedis.JedisCluster;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

@CjService(name = "defaultInnerBehaviorService")
public class DefaultInnerBehaviorService extends AbstractService implements IInnerBehaviorService {
    @CjServiceRef(refByName = "@.redis.cluster")
    JedisCluster jedisCluster;


    @Override
    public void putRedisItems(String pool, TrafficDashboardPointer trafficDashBoardPointer) throws CircuitException {
        int limit = 100;
        int offset = 0;
        long lastBubbleTime = trafficDashBoardPointer.getLastBubbleTime();

        ItemBehaviorPointer innerBehaviorPointer = trafficDashBoardPointer.getInnerBehaviorPointer();
        while (true) {
            List<ItemBehavior> behaviors = pageBehavior(pool, lastBubbleTime, limit, offset);
            if (behaviors.isEmpty()) {
                break;
            }
            offset += behaviors.size();
            for (ItemBehavior behavior : behaviors) {
                if (new BigDecimal(behavior.getLikes()).compareTo(innerBehaviorPointer.getLikesRatio()) <= 0
                        && new BigDecimal(behavior.getComments()).compareTo(innerBehaviorPointer.getCommentsRatio()) <= 0
                        && new BigDecimal(behavior.getRecommends()).compareTo(innerBehaviorPointer.getRecommendsRatio()) <= 0) {
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
                limit, offset, ItemBehavior._COL_NAME_INNER, ItemBehavior.class.getName(), beginTIme);
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
                ItemBehavior._COL_NAME_INNER, ItemBehavior.class.getName(), item);
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
}
