package cj.netos.chasechain.bubbler.ports;

import cj.netos.chasechain.bubbler.ITrafficDashboardService;
import cj.netos.chasechain.bubbler.TrafficDashboardPointer;
import cj.netos.chasechain.bubbler.TrafficeBashboradResult;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.studio.ecm.net.CircuitException;
import cj.studio.openport.ISecuritySession;

@CjService(name = "/dashboard.ports")
public class DefaultTrafficDashboardPorts implements ITrafficDashboardPorts {
    @CjServiceRef(refByName = "defaultTrafficDashboardService")
    ITrafficDashboardService trafficDashboardService;

    @Override
    public TrafficeBashboradResult getDashboard(ISecuritySession securitySession, String poolId) throws CircuitException {
        TrafficDashboardPointer pointer = trafficDashboardService.getPointer(poolId);
        return pointer.copyTo(poolId);
    }
}
