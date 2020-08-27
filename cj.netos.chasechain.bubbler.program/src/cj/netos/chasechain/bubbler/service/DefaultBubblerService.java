package cj.netos.chasechain.bubbler.service;

import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.cube.framework.IDocument;
import cj.lns.chip.sos.cube.framework.IQuery;
import cj.lns.chip.sos.cube.framework.TupleDocument;
import cj.netos.chasechain.bubbler.*;
import cj.studio.ecm.CJSystem;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.studio.ecm.net.CircuitException;
import cj.ultimate.gson2.com.google.gson.Gson;
import org.bson.Document;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@CjService(name = "defaultBubblerService")
public class DefaultBubblerService extends AbstractService implements IBubblerService {
    @CjServiceRef(refByName = "@.redis.cluster")
    JedisCluster jedisCluster;
    @CjServiceRef(refByName = "defaultInnateBehaviorService")
    IInnateBehaviorService innateBehaviorService;
    @CjServiceRef(refByName = "defaultInnerBehaviorService")
    IInnerBehaviorService innerBehaviorService;
    @CjServiceRef(refByName = "defaultTrafficPersonService")
    ITrafficPersonService trafficPersonService;
    @Override
    public void bubble(TrafficDashboardPointer pointer, TrafficPool sourcePool, TrafficPool parentPool) throws CircuitException {
        //获取需要抽取的item ids
        ICube sourceCube = cube(sourcePool.getId());
        ICube parentCube = cube(parentPool.getId());
        //在putRedisItems方法中以行为和时间判断是否放入redis
        Set<String> itemIds = jedisCluster.smembers(Constants.set_bubbler_items_redis_key);
        CJSystem.logging().info(getClass(), String.format("发现可冒泡的内容项条目数是：%s 在流量池：%s[%s]:", itemIds.size(), sourcePool.getTitle(), sourcePool.getId()));
        int limit = 100;
        long offset = 0;
        long realBubbleItemCount=0;
        while (true) {
            List<ContentItem> items = pageItem(sourceCube, itemIds, limit, offset);
            if (items.isEmpty()) {
                break;
            }
            offset += items.size();
            for (ContentItem item : items) {
                //如果父池中已存在该内容物则仅更新其基本行为
                if (existsItemInParentPool(item.getId(), parentCube)) {
                    updateBehaviors(sourcePool, item, parentPool);
                    continue;
                }
                bubbleItemToParent(sourcePool, sourceCube, item, parentPool, parentCube);
                realBubbleItemCount++;
            }
        }
        CJSystem.logging().info(getClass(), String.format("实际已冒泡的内容项条目数是：%s 在流量池：%s[%s]:", (realBubbleItemCount), sourcePool.getTitle(), sourcePool.getId()));
    }

    private boolean existsItemInParentPool(String itemId, ICube parentCube) {
        return parentCube.tupleCount(ContentItem._COL_NAME, String.format("{'tuple.id':'%s'}", itemId)) > 0;
    }

    private void bubbleItemToParent(TrafficPool sourcePool, ICube sourceCube, ContentItem item, TrafficPool parentPool, ICube parentCube) throws CircuitException {
        //copy item
        //copy box of item
        //copy inner and innate behaviors of item to innate behaviors of parentPool
        copyItem(sourcePool, sourceCube, item, parentPool, parentCube);
        copyBox(sourcePool, sourceCube, item, parentPool, parentCube);
        copyBehaviors(sourcePool, item, parentPool);
        copyPersons(sourcePool, sourceCube, item, parentPool, parentCube);
    }

    private void copyPersons(TrafficPool sourcePool, ICube sourceCube, ContentItem item, TrafficPool parentPool, ICube parentCube) throws CircuitException {
        if (trafficPersonService.existsPerson(parentPool.getId(), item.getPointer().getCreator())) {
            return;
        }
        trafficPersonService.addTrafficPerson(parentPool.getId(),item.getPointer().getCreator());
    }
    private void updateBehaviors(TrafficPool sourcePool, ContentItem item, TrafficPool parentPool) throws CircuitException {
        ItemBehavior innateBehavior = innateBehaviorService.getBehavior(sourcePool.getId(), item.getId());
        ItemBehavior innerBehavior = innerBehaviorService.getBehavior(sourcePool.getId(), item.getId());
        ItemBehavior parentInnateBehavior = new ItemBehavior();
        parentInnateBehavior.setItem(item.getId());
        parentInnateBehavior.setUtime(System.currentTimeMillis());
        parentInnateBehavior.setLikes(innateBehavior.getLikes() + innerBehavior.getLikes());
        parentInnateBehavior.setComments(innateBehavior.getComments() + innerBehavior.getComments());
        parentInnateBehavior.setRecommends(innateBehavior.getRecommends() + innerBehavior.getRecommends());
        innateBehaviorService.upateBehavior(parentPool.getId(), parentInnateBehavior);
    }
    private void copyBehaviors(TrafficPool sourcePool, ContentItem item, TrafficPool parentPool) throws CircuitException {
        ItemBehavior innateBehavior = innateBehaviorService.getBehavior(sourcePool.getId(), item.getId());
        ItemBehavior innerBehavior = innerBehaviorService.getBehavior(sourcePool.getId(), item.getId());
        ItemBehavior parentInnateBehavior = new ItemBehavior();
        parentInnateBehavior.setItem(item.getId());
        parentInnateBehavior.setUtime(System.currentTimeMillis());
        parentInnateBehavior.setLikes(innateBehavior.getLikes() + innerBehavior.getLikes());
        parentInnateBehavior.setComments(innateBehavior.getComments() + innerBehavior.getComments());
        parentInnateBehavior.setRecommends(innateBehavior.getRecommends() + innerBehavior.getRecommends());
        innateBehaviorService.addBehavior(parentPool.getId(), parentInnateBehavior);
    }


    private void copyBox(TrafficPool sourcePool, ICube sourceCube, ContentItem item, TrafficPool parentPool, ICube parentCube) {
        //由于一个内容盒包含多个消息，所以冒泡时要检查是否盒子已存在了，不存在才拷贝
        if (parentCube.tupleCount(ContentBox._COL_NAME, String.format("{'tuple.id':'%s'}", item.getBox())) > 0) {
            return;
        }
        ContentBox box = getContentBox(sourceCube, item.getBox());
        ContentBox newBox = new ContentBox();
        newBox.setCtime(System.currentTimeMillis());
        newBox.setUpstreamPool(sourcePool.getId());
        newBox.setId(box.getId());
        newBox.setBubbled(false);
        newBox.setLocation(box.getLocation());
        newBox.setPointer(box.getPointer());
        parentCube.saveDoc(ContentBox._COL_NAME, new TupleDocument<>(newBox));
        sourceCube.updateDocOne(ContentBox._COL_NAME,
                Document.parse(String.format("{'tuple.id':'%s'}", item.getId())),
                Document.parse(String.format("{'$set':{'tuple.isBubbled':true}}"))
        );
    }

    private ContentBox getContentBox(ICube sourceCube, String box) {
        String cjql = String.format("select {'tuple':'*'}.limit(1) from tuple %s %s where {'tuple.id':'%s'}",
                ContentBox._COL_NAME, ContentBox.class.getName(), box);
        IQuery<ContentBox> query = sourceCube.createQuery(cjql);
        IDocument<ContentBox> document = query.getSingleResult();
        if (document == null) {
            return null;
        }
        return document.tuple();
    }

    private void copyItem(TrafficPool sourcePool, ICube sourceCube, ContentItem item, TrafficPool parentPool, ICube parentCube) {
        ContentItem newItem = new ContentItem();
        newItem.setBox(item.getBox());
        newItem.setCtime(System.currentTimeMillis());
        newItem.setLocation(item.getLocation());
        newItem.setPointer(item.getPointer());
        newItem.setUpstreamPool(sourcePool.getId());
        newItem.setId(item.getId());
        newItem.setBubbled(false);
        parentCube.saveDoc(ContentItem._COL_NAME, new TupleDocument<>(newItem));
        sourceCube.updateDocOne(ContentItem._COL_NAME,
                Document.parse(String.format("{'tuple.id':'%s'}", item.getId())),
                Document.parse(String.format("{'$set':{'tuple.isBubbled':true}}"))
        );
    }

    private List<ContentItem> pageItem(ICube sourceCube,  Set<String> itemIds, int limit, long offset) {
        String cjql = String.format("select {'tuple':'*'}.limit(%s).skip(%s) from tuple %s %s where {'tuple.id':{'$in':%s}}",
                limit, offset, ContentItem._COL_NAME, ContentItem.class.getName(), new Gson().toJson(itemIds));
        IQuery<ContentItem> query = sourceCube.createQuery(cjql);
        List<IDocument<ContentItem>> list = query.getResultList();
        List<ContentItem> contentItems = new ArrayList<>();
        for (IDocument<ContentItem> document : list) {
            contentItems.add(document.tuple());
        }
        return contentItems;
    }
}
