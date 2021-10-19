package org.apache.ignite.internal.management.baseline;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.Command;
import org.apache.ignite.internal.management.Parameter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;

/** */
@Command(name = "add",
    commandDescription = "Add nodes to baseline topology.")
public class BaselineAddCommand implements IgniteCallable<String> {
    /** Auto-injected Ignite instance. */
    @IgniteInstanceResource
    private IgniteEx ignite;

    /** Parameter will be injected on command instantiation. Default comma separation. */
    @Parameter(names = {"--nodes", "-n"}, description = "List of baseline nodes to add.")
    private List<String> consistentIds;

    /** {@inheritDoc} */
    @Override public String call() throws Exception {
        Collection<BaselineNode> baseline = ignite.cluster().currentBaselineTopology();
        Collection<ClusterNode> srvs = ignite.cluster().forServers().nodes();

        for (String consistentId : consistentIds) {
            ClusterNode node = F.find(srvs, null, new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return node.consistentId().toString().equals(consistentId);
                }
            });

            if (node == null)
                throw new IllegalArgumentException("Node not found for consistent ID: " + consistentId);

            baseline.add(node);
        }

        ignite.cluster().setBaselineTopology(baseline);

        return consistentIds.toString();
    }
}
