package cj.netos.chasechain.bubbler;

import cj.lns.chip.sos.cube.framework.CubeConfig;
import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.disk.INetDisk;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.studio.ecm.net.CircuitException;
import org.bson.Document;

public class AbstractService {
    private static transient final String _COL_NAME_dashboard = "traffic.dashboard.pointers";
    @CjServiceRef(refByName = "mongodb.netos")
    INetDisk disk;

    protected ICube cube(String pool) throws CircuitException {
        String col = String.format("%s.%s", TrafficPool._COL_NAME, pool);
        if (!disk.existsCube(col)) {
            CubeConfig cubeConfig = new CubeConfig();
            cubeConfig.alias(col);
            ICube cube= disk.createCube(col, cubeConfig);
            cube.createIndex(_COL_NAME_dashboard, Document.parse(String.format("{'tuple.lastBubbleTime':-1}")));
            return cube;
        }
        return disk.cube(col);
    }
}
