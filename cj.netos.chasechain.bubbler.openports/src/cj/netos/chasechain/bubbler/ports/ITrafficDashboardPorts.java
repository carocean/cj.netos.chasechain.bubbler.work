package cj.netos.chasechain.bubbler.ports;

import cj.netos.chasechain.bubbler.TrafficeBashboradResult;
import cj.studio.ecm.net.CircuitException;
import cj.studio.openport.IOpenportService;
import cj.studio.openport.ISecuritySession;
import cj.studio.openport.annotations.CjOpenport;
import cj.studio.openport.annotations.CjOpenportParameter;
import cj.studio.openport.annotations.CjOpenports;

@CjOpenports(usage = "流量仪表盘")
public interface ITrafficDashboardPorts extends IOpenportService {
    @CjOpenport(usage = "获取指定流量池的仪表盘")
    TrafficeBashboradResult getDashboard(ISecuritySession securitySession,
                                         @CjOpenportParameter(usage = "流量池标识", name = "poolId") String poolId) throws CircuitException;
}
