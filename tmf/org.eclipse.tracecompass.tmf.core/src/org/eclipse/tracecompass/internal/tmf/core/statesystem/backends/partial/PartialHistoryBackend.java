/*******************************************************************************
 * Copyright (c) 2013, 2023 Ericsson
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License 2.0 which
 * accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *   Alexandre Montplaisir - Initial API and implementation
 *   Patrick Tasse - Add message to exceptions
 *   Matthew Khouzam - Make state system work in a single pass
 *   Fabien Reumont-Locke - Improve single pass generation
 *******************************************************************************/

package org.eclipse.tracecompass.internal.tmf.core.statesystem.backends.partial;

import static org.eclipse.tracecompass.common.core.NonNullUtils.checkNotNullContents;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.common.core.log.TraceCompassLog;
import org.eclipse.tracecompass.common.core.log.TraceCompassLogUtils;
import org.eclipse.tracecompass.internal.provisional.datastore.core.condition.IntegerRangeCondition;
import org.eclipse.tracecompass.internal.provisional.datastore.core.condition.TimeRangeCondition;
import org.eclipse.tracecompass.internal.tmf.core.Activator;
import org.eclipse.tracecompass.statesystem.core.ITmfStateSystem;
import org.eclipse.tracecompass.statesystem.core.backend.IPartialStateHistoryBackend;
import org.eclipse.tracecompass.statesystem.core.backend.IStateHistoryBackend;
import org.eclipse.tracecompass.statesystem.core.exceptions.StateSystemDisposedException;
import org.eclipse.tracecompass.statesystem.core.exceptions.TimeRangeException;
import org.eclipse.tracecompass.statesystem.core.interval.ITmfStateInterval;
import org.eclipse.tracecompass.tmf.core.event.ITmfEvent;
import org.eclipse.tracecompass.tmf.core.request.ITmfEventRequest;
import org.eclipse.tracecompass.tmf.core.request.TmfEventRequest;
import org.eclipse.tracecompass.tmf.core.statesystem.AbstractTmfStateProvider;
import org.eclipse.tracecompass.tmf.core.statesystem.ITmfStateProvider;
import org.eclipse.tracecompass.tmf.core.timestamp.TmfTimeRange;
import org.eclipse.tracecompass.tmf.core.timestamp.TmfTimestamp;
import org.eclipse.tracecompass.tmf.core.trace.ITmfTrace;

/**
 * Partial state history back-end.
 *
 * This is a shim inserted between the real state system and a "real" history
 * back-end. It will keep checkpoints, everytime the insertPastState() method
 * meets the CHECKPOINT_ATTRIBUTE quark, and will only forward to the real state
 * history the state intervals that crosses at least one checkpoint. Every other
 * interval will be discarded. The CHECKPOINT_ATTRIBUTE plays a "clock signal"
 * role, it tells the backend when to save checkpoints to the real state
 * history.
 *
 * This would mean that it can only answer queries exactly at the checkpoints.
 * For any other timestamps (ie, most of the time), it will load the closest
 * earlier checkpoint, and will re-feed the state-change-input with events from
 * the trace, to restore the real state at the time that was requested.
 *
 * @author Alexandre Montplaisir
 */
public class PartialHistoryBackend implements IStateHistoryBackend {

    private final String fSSID;

    private static final Logger LOGGER = TraceCompassLog.getLogger(PartialHistoryBackend.class);

    /**
     * A partial history needs the state input plugin to re-generate state
     * between checkpoints.
     */
    private final ITmfStateProvider fPartialInput;

    /**
     * Fake state system that is used for partially rebuilding the states (when
     * going from a checkpoint to a target query timestamp).
     */
    private final PartialStateSystem fPartialSS;

    /** Reference to the "real" state history that is used for storage */
    private final IStateHistoryBackend fInnerHistory;

    /** Checkpoints set, <Timestamp> */
    private final TreeSet<Long> fCheckpoints = new TreeSet<>();

    private long fLatestTime;
    private int fQuarkCounter = 0;

    private final IPartialStateHistoryBackend fBackend;

    /** Has at least one state been inserted when it is "true" */
    private volatile boolean fInitialized = false;

    /** The quark of a checkpoint attribute */
    private int fCheckpointQuark;

    private long fGranularity;

    /**
     * Constructor
     *
     * @param ssid
     *            The state system's ID
     * @param partialInput
     *            The state change input object that was used to build the
     *            upstream state system. This partial history will make its own
     *            copy (since they have different targets).
     * @param pss
     *            The partial history's inner state system. It should already be
     *            assigned to partialInput.
     * @param realBackend
     *            The real state history back-end to use. It's supposed to be
     *            modular, so it should be able to be of any type.
     * @param granularity
     *            Configuration parameter indicating how many trace events there
     *            should be between each checkpoint
     * @param backend
     *            The backend used for storage
     */
    public PartialHistoryBackend(String ssid,
            ITmfStateProvider partialInput,
            PartialStateSystem pss,
            IStateHistoryBackend realBackend,
            long granularity,
            IPartialStateHistoryBackend backend) {
        if (granularity <= 0 || partialInput.getAssignedStateSystem() != pss) {
            throw new IllegalArgumentException();
        }

        final long startTime = realBackend.getStartTime();

        fSSID = ssid;
        fPartialInput = partialInput;
        fPartialSS = pss;
        fGranularity = granularity;

        fInnerHistory = realBackend;
        fBackend = backend;

        fLatestTime = startTime;
    }

    @Override
    public String getSSID() {
        return fSSID;
    }

    @Override
    public long getStartTime() {
        return fInnerHistory.getStartTime();
    }

    @Override
    public long getEndTime() {
        return fLatestTime;
    }

    @Override
    public void insertPastState(long stateStartTime, long stateEndTime,
            int quark, @Nullable Object value) throws TimeRangeException {
        if (!fInitialized) {
            fCheckpointQuark = fPartialSS.getQuarkAbsoluteAndAdd(PartialStateSystem.CHECKPOINT_ATTRIBUTE);
            fCheckpoints.add(fPartialInput.getStartTime());
            fInitialized = true;
        }
        /* Ignore checkpoint modification */
        if (quark == fCheckpointQuark) {
            return;
        }
        if (quark > fQuarkCounter) {
            fQuarkCounter++;
        }
        /* Update the latest time */
        if (stateEndTime > fLatestTime) {
            fLatestTime = stateEndTime;
        }

        addCheckpointInnerBackendTimeBased();

        /*
         * Check if the interval intersects the previous checkpoint. If so,
         * insert it in the real history back-end.
         *
         * FIXME since intervals are inserted in order of rank, we could avoid
         * doing a map lookup every time here (just compare with the known
         * previous one).
         */
        if (stateStartTime <= fCheckpoints.floor(stateEndTime)) {
            fInnerHistory.insertPastState(stateStartTime, stateEndTime, quark, value);
        }
    }

    private void addCheckpointInnerBackendTimeBased() {
        while (fLatestTime >= fCheckpoints.last() + fGranularity) {
            long lastCheckpointTime = fCheckpoints.last();
            long nextCheckpointTime = lastCheckpointTime + fGranularity;
            fCheckpoints.add(nextCheckpointTime);
            fInnerHistory.insertPastState(lastCheckpointTime, nextCheckpointTime - 1, fCheckpointQuark, fCheckpoints.size() - 1);
        }
    }

    @Override
    public void finishedBuilding(long endTime) throws TimeRangeException {
        if (endTime > fCheckpoints.last()) {
            fInnerHistory.insertPastState(fCheckpoints.last(), endTime, fCheckpointQuark, fCheckpoints.size());
            fCheckpoints.add(endTime);
            fInnerHistory.finishedBuilding(endTime);
        } else {
            fInnerHistory.finishedBuilding(endTime);
        }
    }

    @Override
    public @Nullable FileInputStream supplyAttributeTreeReader() {
        return fInnerHistory.supplyAttributeTreeReader();
    }

    @Override
    public @Nullable File supplyAttributeTreeWriterFile() {
        return fInnerHistory.supplyAttributeTreeWriterFile();
    }

    @Override
    public long supplyAttributeTreeWriterFilePosition() {
        return fInnerHistory.supplyAttributeTreeWriterFilePosition();
    }

    @Override
    public void removeFiles() {
        fInnerHistory.removeFiles();
    }

    @Override
    public void dispose() {
        fPartialInput.dispose();
        fPartialSS.dispose();
        fInnerHistory.dispose();
    }

    @Override
    public void doQuery(List<@Nullable ITmfStateInterval> currentStateInfo, long t)
            throws TimeRangeException, StateSystemDisposedException {
        try (TraceCompassLogUtils.ScopeLog log = new TraceCompassLogUtils.ScopeLog(LOGGER, Level.FINEST, "PartialHistoryBackend:doQuery", //$NON-NLS-1$
                "time", t)) { //$NON-NLS-1$
            checkValidTime(t);
            ITmfStateSystem upstreamSS = fPartialSS.getUpstreamSS();
            if (upstreamSS == null) {
                throw new StateSystemDisposedException();
            }
            upstreamSS.waitUntilBuilt();

            /* Reload the previous checkpoint */
            long checkpointBefore = Objects.requireNonNull(fCheckpoints.floor(t));
            /*
             * Add state if the number of elements is less than the number of
             * existing quarks
             */
            while (currentStateInfo.size() < getNbAttributes()) {
                currentStateInfo.add(null);
            }
            fInnerHistory.doQuery(currentStateInfo, checkpointBefore);

            /*
             * If the request is at the end, some intervals might not have been
             * recorded, since the checkpoint is created after all the intervals
             * are inserted.
             */
            if (currentStateInfo.contains(null)) {
                checkpointBefore = Objects.requireNonNull(fCheckpoints.floor(t - 1));
                fInnerHistory.doQuery(currentStateInfo, checkpointBefore);
            }

            /*
             * If all intervals cover the timestamp t requested then we do not
             * need to read the trace.
             */
            boolean isStateInfoFull = true;
            for (ITmfStateInterval interval : currentStateInfo) {
                if (Objects.requireNonNull(interval).getEndTime() < t) {
                    isStateInfoFull = false;
                    break;
                }
            }
            if (isStateInfoFull) {
                return;
            }
            List<ITmfStateInterval> previousCheckpointStateInfo = checkNotNullContents(currentStateInfo.stream()).collect(Collectors.toList());
            /*
             * Load the next checkpoint if we are not at the end as states are
             * not recorded at the last checkpoint
             */
            long checkpointAfter = Objects.requireNonNull(fCheckpoints.ceiling(t));
            if (t < fLatestTime) {
                List<@Nullable ITmfStateInterval> nextCheckpointStateInfo = prepareIntervalList(currentStateInfo.size());
                fInnerHistory.doQuery(nextCheckpointStateInfo, checkpointAfter);

                /*
                 * If all intervals cover the timestamp t requested then we do
                 * not need to read the trace.
                 */
                isStateInfoFull = true;
                for (int i = 0; i < currentStateInfo.size(); ++i) {
                    ITmfStateInterval currentInterval = currentStateInfo.get(i);
                    if (currentInterval == null || currentInterval.getEndTime() < t) {
                        currentInterval = nextCheckpointStateInfo.get(i);
                        if (currentInterval == null || currentInterval.getStartTime() > t) {
                            isStateInfoFull = false;
                            break;
                        }
                        currentStateInfo.set(i, nextCheckpointStateInfo.get(i));
                    }
                }
                if (isStateInfoFull) {
                    return;
                }
            }

            fPartialSS.takeQueryLock();
            try {
                /*
                 * Set the initial contents of the partial state system (which
                 * is the contents of the query at the checkpoint).
                 */
                fPartialSS.setNbAttributes(currentStateInfo.size());
                fPartialSS.replaceOngoingState(previousCheckpointStateInfo);

                /*
                 * Send an event request to update the state system.
                 */
                TmfTimeRange range = new TmfTimeRange(
                        /*
                         * The state at the checkpoint already includes any
                         * state change caused by the event(s) happening exactly
                         * at 'checkpointTime', if any. We must not include
                         * those events in the query.
                         */
                        TmfTimestamp.fromNanos(checkpointBefore + 1),
                        TmfTimestamp.fromNanos(checkpointAfter));
                ITmfEventRequest request = new PartialStateSystemRequest(fPartialInput, range);
                fPartialInput.getTrace().sendRequest(request);

                Logger logger = Logger.getAnonymousLogger();
                try {
                    request.waitForCompletion();
                } catch (InterruptedException e) {
                    logger.log(Level.SEVERE, "A InterruptedException exception occurred", e); //$NON-NLS-1$
                }

                if (fLatestTime == t) {
                    fPartialSS.closeHistory(t);
                }

                /*
                 * Now, we have the intervals with their real end times written
                 * to the backend, we should be able to get them from there
                 */
                List<ITmfStateInterval> intervalsList = ((ITmfStateSystem) fPartialSS).queryFullState(t);

                for (int i = 0; i < intervalsList.size(); i++) {
                    ITmfStateInterval interval = intervalsList.get(i);
                    /*
                     * Because intervals are disjoint, if t == endTime or t ==
                     * startTime, then the interval contains t.
                     */
                    if (interval != null && interval.getEndTime() >= t && interval.getStartTime() <= t) {
                        currentStateInfo.set(i, intervalsList.get(i));
                    }
                }
            } finally {
                fPartialSS.releaseQueryLock();
            }
        }
    }

    private static List<@Nullable ITmfStateInterval> prepareIntervalList(int nbAttrib) {
        return new ArrayList<>(Collections.nCopies(nbAttrib, null));
    }

    @Override
    public ITmfStateInterval doSingularQuery(long t, int attributeQuark) throws TimeRangeException, StateSystemDisposedException {
        checkValidTime(t);
        try (TraceCompassLogUtils.ScopeLog log = new TraceCompassLogUtils.ScopeLog(LOGGER, Level.FINEST, "PartialHistoryBackend:doSingularQuery", //$NON-NLS-1$
                "quark", attributeQuark, //$NON-NLS-1$
                "time", t)) { //$NON-NLS-1$
            ITmfStateSystem upstreamSS = fPartialSS.getUpstreamSS();
            if (upstreamSS == null) {
                throw new StateSystemDisposedException();
            }
            upstreamSS.waitUntilBuilt();

            /* Reload the previous checkpoint */
            long checkpointTime = Objects.requireNonNull(fCheckpoints.floor(t));
            int nbAttributes = getNbAttributes();
            List<@Nullable ITmfStateInterval> intervalsList = prepareIntervalList(nbAttributes);

            /* Checking if the interval was stored in the real backend */
            fInnerHistory.doQuery(intervalsList, checkpointTime);
            ITmfStateInterval ret = intervalsList.get(attributeQuark);

            if (ret == null || !ret.intersects(t)) {
                /*
                 * The interval was not stored on disk, read it from the trace
                 * then
                 */
                intervalsList = prepareIntervalList(nbAttributes);
                this.doQuery(intervalsList, t);
                ret = Objects.requireNonNull(intervalsList.get(attributeQuark));
            }
            return ret;
        }
    }

    @Override
    public Iterable<ITmfStateInterval> query2D(@Nullable IntegerRangeCondition quarks, @Nullable TimeRangeCondition times)
            throws TimeRangeException {
        if (times == null || quarks == null) {
            return Collections.emptyList();
        }
        long[] timeArray = times.getTimeArray();

        /*
         * Getting the lower and upper checkpoint timestamps that bound the time
         * range condition
         */
        long lowerCheckpoint = Objects.requireNonNull(fCheckpoints.floor(times.min()));
        Long upperCheckpoint = fCheckpoints.ceiling(times.max());
        if (upperCheckpoint == null) {
            upperCheckpoint = Collections.max(fCheckpoints);
        }
        // We should be able to know if a transition happened between two pixels
        // if we sample 2 times more than the requested frequency.
        if (timeArray.length > 1 && (timeArray[1] - timeArray[0]) >= 2 * fGranularity) {
            Long step = (timeArray[1] - timeArray[0]) / fGranularity * fGranularity;
            TimeRangeCondition adjustedTimes = TimeRangeCondition.forDiscreteRange(
                    LongStream.iterate(lowerCheckpoint, i -> i + step).limit((upperCheckpoint - lowerCheckpoint) / step).boxed().toList());
            return fInnerHistory.query2D(quarks, adjustedTimes);
        }

        /* Querying the partial history at the lowerCheckpoint */
        List<@Nullable ITmfStateInterval> currentStateInfo = prepareIntervalList(getNbAttributes());
        try {
            fInnerHistory.doQuery(currentStateInfo, lowerCheckpoint);
        } catch (StateSystemDisposedException e) {
            Activator.logError(e.getMessage(), e);
        }

        List<ITmfStateInterval> filledStateInfo = checkNotNullContents(currentStateInfo.stream()).collect(Collectors.toList());
        // Do query from the trace directly
        ITmfStateSystem upstreamSS = fPartialSS.getUpstreamSS();
        if (upstreamSS == null) {
            return Collections.emptyList();
        }
        upstreamSS.waitUntilBuilt();

        try {
            fPartialSS.takeQueryLock();
            fPartialSS.setNbAttributes(currentStateInfo.size());
            fPartialSS.replaceOngoingState(filledStateInfo);
            /*
             * Updating the backend of fPartialSS with the quarks and time
             * ranges
             */
            fBackend.updateRangeCondition(quarks);
            fBackend.updateTimeCondition(times);
            fBackend.updateQueryType(true);
            /*
             * Reading the trace updating the state until the upperCheckpoint to
             * get the missing intervals
             */
            TmfTimeRange range = new TmfTimeRange(TmfTimestamp.fromNanos(lowerCheckpoint + 1), TmfTimestamp.fromNanos(upperCheckpoint));

            ITmfEventRequest request = new PartialStateSystemRequest(fPartialInput, range);
            fPartialInput.getTrace().sendRequest(request);

            /*
             * Querying the intervals read from the trace and adding them to the
             * output
             */
            request.waitForCompletion();
            return ((ITmfStateSystem) fPartialSS).query2D(Arrays.stream(quarks.getIntegerArray()).boxed().toList(), times.min(), times.max());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IndexOutOfBoundsException | StateSystemDisposedException e) {
            Activator.logError(e.getMessage(), e);
        } finally {
            fBackend.updateQueryType(false);
            fPartialSS.releaseQueryLock();
        }
        return Collections.emptyList();
    }

    private void checkValidTime(long t) {
        long startTime = getStartTime();
        long endTime = getEndTime();
        if (t < startTime || t > endTime) {
            throw new TimeRangeException(String.format("%s Time:%d, Start:%d, End:%d", //$NON-NLS-1$
                    fSSID, t, startTime, endTime));
        }
    }

    private int getNbAttributes() {
        return fPartialSS.getAttributeTree().getNbAttributes();
    }

    // ------------------------------------------------------------------------
    // Event requests types
    // ------------------------------------------------------------------------

    private class PartialStateSystemRequest extends TmfEventRequest {
        private final ITmfStateProvider sci;
        private final ITmfTrace trace;

        PartialStateSystemRequest(ITmfStateProvider sci, TmfTimeRange range) {
            super(ITmfEvent.class,
                    range,
                    0,
                    ITmfEventRequest.ALL_DATA,
                    ITmfEventRequest.ExecutionType.FOREGROUND);
            this.sci = sci;
            this.trace = sci.getTrace();
        }

        @Override
        public void handleData(final ITmfEvent event) {
            super.handleData(event);
            if (event.getTrace() == trace) {
                sci.processEvent(event);
            }
        }

        @Override
        public void handleCompleted() {
            /*
             * If we're using a threaded state provider, we need to make sure
             * all events have been handled by the state system before doing
             * queries on it.
             */
            if (fPartialInput instanceof AbstractTmfStateProvider) {
                ((AbstractTmfStateProvider) fPartialInput).waitForEmptyQueue();
            }
            super.handleCompleted();
        }

    }
}
