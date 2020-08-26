package cj.netos.chasechain.bubbler;

import cj.studio.ecm.net.CircuitException;

public interface ITrafficDashboardService {

    TrafficDashboardPointer getPointer(String pool) throws CircuitException;

    void movePointer(String pool, TrafficDashboardPointer fromPointer, long lastItemTime) throws CircuitException;

    void clearPointersExceptTop(String poolId, int retains) throws CircuitException;

}
