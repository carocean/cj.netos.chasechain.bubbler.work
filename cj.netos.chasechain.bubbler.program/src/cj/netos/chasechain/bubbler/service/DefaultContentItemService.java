package cj.netos.chasechain.bubbler.service;

import cj.lns.chip.sos.cube.framework.ICube;
import cj.netos.chasechain.bubbler.AbstractService;
import cj.netos.chasechain.bubbler.ContentItem;
import cj.netos.chasechain.bubbler.IContentItemService;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.net.CircuitException;

@CjService(name = "defaultContentItemService")
public class DefaultContentItemService extends AbstractService implements IContentItemService {
    @Override
    public long totalCount(String pool, long beginTime) throws CircuitException {
        ICube cube = cube(pool);
        return cube.tupleCount(ContentItem._COL_NAME, String.format("{'tuple.ctime':%s}", beginTime));
    }
}
