package cj.netos.chasechain.bubbler;

import cj.studio.ecm.net.CircuitException;

public interface IContentItemService {
    long totalCount(String pool, long beginTime) throws CircuitException;
}
