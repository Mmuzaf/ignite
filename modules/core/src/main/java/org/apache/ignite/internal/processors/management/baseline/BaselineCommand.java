package org.apache.ignite.internal.processors.management.baseline;

import java.util.Collection;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.cluster.baseline.autoadjust.BaselineAutoAdjustStatus;
import org.apache.ignite.internal.processors.management.Command;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 *
 */
@Command(name = "baseline",
    commandDescription = "Baseline Command",
    subcommands = {BaselineAddCommand.class})
public class BaselineCommand implements IgniteCallable<VisorBaselineTaskResult> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Auto-injected Ignite instance. */
    @IgniteInstanceResource
    private IgniteEx ignite;

    /** {@inheritDoc} */
    @Override public VisorBaselineTaskResult call() throws Exception {
        IgniteClusterEx cluster = ignite.cluster();

        Collection<? extends BaselineNode> baseline = cluster.currentBaselineTopology();
        Collection<? extends BaselineNode> srvrs = cluster.forServers().nodes();

        VisorBaselineAutoAdjustSettings autoAdjustSettings = new VisorBaselineAutoAdjustSettings(
            cluster.isBaselineAutoAdjustEnabled(),
            cluster.baselineAutoAdjustTimeout()
        );

        BaselineAutoAdjustStatus adjustStatus = cluster.baselineAutoAdjustStatus();

        return new VisorBaselineTaskResult(
            ignite.cluster().active(),
            cluster.topologyVersion(),
            F.isEmpty(baseline) ? null : baseline,
            srvrs,
            autoAdjustSettings,
            adjustStatus.getTimeUntilAutoAdjust(),
            adjustStatus.getTaskState() == BaselineAutoAdjustStatus.TaskState.IN_PROGRESS
        );
    }
}
