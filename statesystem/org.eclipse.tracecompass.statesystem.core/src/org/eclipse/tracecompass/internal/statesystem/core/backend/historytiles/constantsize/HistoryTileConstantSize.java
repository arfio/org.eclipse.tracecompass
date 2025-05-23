package org.eclipse.tracecompass.internal.statesystem.core.backend.historytiles.constantsize;

import java.util.List;

import org.eclipse.tracecompass.statesystem.core.interval.ITmfStateInterval;
import org.eclipse.tracecompass.statesystem.core.statevalue.ITmfStateValue;

public class HistoryTileConstantSize {


    private long[] fStartTimes;
    private long[] fEndTimes;
    private List<ITmfStateValue> fStateValues;

    public void insertInterval(long start, long end, int quark, Object value) {

    }

    public Iterable<ITmfStateInterval> query2d() {

    }
    public List<ITmfStateInterval> fullQuery() {

    }
    public ITmfStateInterval singleStateQuery() {

    }
}
