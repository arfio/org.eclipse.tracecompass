package org.eclipse.tracecompass.internal.statesystem.core.backend.historytiles.dynamic;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.common.core.log.TraceCompassLog;
import org.eclipse.tracecompass.datastore.core.encoding.HTVarInt;
import org.eclipse.tracecompass.internal.provisional.datastore.core.condition.IntegerRangeCondition;
import org.eclipse.tracecompass.internal.provisional.datastore.core.condition.TimeRangeCondition;
import org.eclipse.tracecompass.statesystem.core.interval.ITmfStateInterval;
import org.eclipse.tracecompass.traceeventlogger.LogUtils;

/**
 * @since 5.4
 *
 */
public class HistoryTile {

    private static final @NonNull Logger LOGGER = TraceCompassLog.getLogger(HistoryTile.class);
    private Map<Integer, List<@NonNull ITmfStateInterval>> fIntervalMap = new HashMap<>();
    private long fResolution;
    private long fStart;
    private long fEnd;
    private int fNPixels = 0;
    private int fDuplicatedSize = 0;
    private int fDuplicatedIntervals = 0;
    private boolean fFinished = false;
    private boolean fIgnoreResolutionCutOff = false;
    private int fSize = 0;

    /* Lock used to protect the accesses to intervals, nodeEnd and such */
    private final ReentrantReadWriteLock fRwl = new ReentrantReadWriteLock(false);

    HistoryTile(long resolution, long start, long end) {
        fResolution = resolution;
        fStart = start;
        fEnd = end;
    }

    HistoryTile(long resolution, long start, long end, int nPixels, boolean ignoreResolutionCutOff) {
        this(resolution, start, end);
        fNPixels = nPixels;
        fIgnoreResolutionCutOff = ignoreResolutionCutOff;
    }

    HistoryTile(long resolution, long start, long end, Map<Integer, List<@NonNull ITmfStateInterval>> intervalMap) {
        this(resolution, start, end);
        fIntervalMap = intervalMap;
        fFinished = true;
    }

    public void writeSelf(FileChannel channel, boolean isEveryIntervalContiguous, String ssid) {
        // to remove 1 line
        LogUtils.traceInstant(LOGGER, Level.FINE, "HistoryTile:writeSelf", "ssid", ssid, "duplicatedDataSize", fDuplicatedSize, "duplicatedIntevals", fDuplicatedIntervals); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        fFinished = true;
        try {
            int nAttributes = fIntervalMap.size();
            int tileSize = (nAttributes * 2 + 2) * Integer.BYTES + fSize;
            if (isEveryIntervalContiguous) {
                for (List<@NonNull ITmfStateInterval> intervalList : fIntervalMap.values()) {
                    tileSize += HTVarInt.getEncodedLengthLong(intervalList.get(0).getStartTime());
                }
            }
            ByteBuffer buffer = ByteBuffer.allocate(tileSize);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.clear();
            buffer.putInt(tileSize);
            buffer.putInt(nAttributes);
            for (Entry<Integer, List<@NonNull ITmfStateInterval>> entry : fIntervalMap.entrySet()) {
                List<@NonNull ITmfStateInterval> intervalList = entry.getValue();
                buffer.putInt(intervalList.size());
                buffer.putInt(entry.getKey());
                if (isEveryIntervalContiguous) {
                    HTVarInt.writeLong(buffer, intervalList.get(0).getStartTime());
                }
                for (ITmfStateInterval interval : intervalList) {
                    ((TileInterval) interval).writeInterval(buffer, isEveryIntervalContiguous);
                }
            }

            /* Finally, write everything in the Buffer to disk */
            buffer.flip();
            int res = channel.write(buffer);
            if (res != tileSize) {
                throw new IllegalStateException("Wrong size of block written: Actual: " + res + ", Expected: " + tileSize); //$NON-NLS-1$ //$NON-NLS-2$
            }
        } catch (IOException | BufferOverflowException e) {
            e.printStackTrace();
        }
    }

    public static HistoryTile readTile(ByteBuffer buffer, long resolution, long start, long end, boolean isEveryIntervalContiguous) {
        Map<Integer, List<@NonNull ITmfStateInterval>> intervalMap = new HashMap<>();
        try {
            buffer.flip();
            buffer.getInt(); // skip tileSize
            int nAttributes = buffer.getInt();

            for (int i = 0; i < nAttributes; i++) {
                int intervalListSize = buffer.getInt();
                int attributeQuark = buffer.getInt();
                List<@NonNull ITmfStateInterval> intervalList = new ArrayList<>(intervalListSize);
                if (isEveryIntervalContiguous) {
                    long startTime = HTVarInt.readLong(buffer);
                    for (int j = 0; j < intervalListSize; j++) {
                        TileInterval interval = TileInterval.readInterval(buffer, startTime, attributeQuark);
                        intervalList.add(interval);
                        startTime += interval.getEndTime() - interval.getStartTime() + 1;
                    }
                } else {
                    for (int j = 0; j < intervalListSize; j++) {
                        TileInterval interval = TileInterval.readInterval(buffer, attributeQuark);
                        intervalList.add(interval);
                    }
                }

                intervalMap.put(attributeQuark, intervalList);
            }
        } catch (BufferUnderflowException | IOException e) {
            e.printStackTrace();
        }
        return new HistoryTile(resolution, start, end, intervalMap);
    }

    public String debugInfo() {
        int size = 0;
        for (Entry<Integer, List<@NonNull ITmfStateInterval>> entry : fIntervalMap.entrySet()) {
            size += entry.getValue().size();
        }
        return "resolution: " + fResolution + ", interval map size: " + size; //$NON-NLS-1$ //$NON-NLS-2$
    }

    public boolean isFinished() {
        return fFinished;
    }

    public long getStart() {
        return fStart;
    }

    public long getEnd() {
        return fEnd;
    }

    public long getResolution() {
        return fResolution;
    }

    public int getNumberAttributes() {
        return fIntervalMap.size();
    }

    public Map<Integer, List<@NonNull ITmfStateInterval>> getIntervalMap() {
        return fIntervalMap;
    }

    public void doQuery(@NonNull List<@Nullable ITmfStateInterval> currentStateInfo, long t) {
        if (t > fEnd) {
            return; // Ignore any interval outside the tile range
        }
        for (int attributeQuark = 0; attributeQuark < currentStateInfo.size(); attributeQuark++) {
            if (fIntervalMap.containsKey(attributeQuark) && currentStateInfo.get(attributeQuark) == null) {
                currentStateInfo.set(attributeQuark, doSingularQuery(t, attributeQuark));
            }
        }
    }

    public ITmfStateInterval doSingularQuery(long t, int attributeQuark) {
        if (t > fEnd) {
            return null; // Ignore any interval outside the tile range
        }

        fRwl.readLock().lock();
        try {
            List<ITmfStateInterval> intervalList = fIntervalMap.getOrDefault(attributeQuark, Collections.emptyList());
            for (ITmfStateInterval interval : intervalList) {
                if (t >= interval.getStartTime() && t <= interval.getEndTime()) {
                    return interval;
                }
            }
            return null;
        } finally {
            fRwl.readLock().unlock();
        }
    }

    public Collection<@NonNull Integer> checkMissingInterval(int[] quarks, long time) {
        Collection<@NonNull Integer> missingIntervalQuarks = new ArrayList<>();
        fRwl.readLock().lock();
        try {
            for (int i = 0; i < quarks.length; i++) {
                List<@NonNull ITmfStateInterval> intervalList = fIntervalMap.getOrDefault(quarks[i], null);
                if (intervalList == null || intervalList.get(intervalList.size() - 1).getEndTime() < time) {
                    missingIntervalQuarks.add(quarks[i]);
                }
            }
        } finally {
            fRwl.readLock().unlock();
        }
        return missingIntervalQuarks;
    }

    public Iterable<@NonNull ITmfStateInterval> query2D(IntegerRangeCondition quarks, TimeRangeCondition times) {
        if (fFinished) {
            return () -> fIntervalMap
                    .entrySet()
                    .stream()
                    .filter(e -> quarks.test(e.getKey()))
                    .map(Entry::getValue)
                    .flatMap(List::stream)
                    .filter(i -> times.intersects(i.getStartTime(), i.getEndTime()))
                    .iterator();
        }
        return Collections.emptyList();
    }

    public void insertPastState(long stateStartTime, long stateEndTime,
            int quark, Object value, boolean isEveryIntervalContiguous) {
        if (stateEndTime < fStart) {
            return; // Ignore any interval outside the tile range
        }
        if (stateEndTime > fEnd) {
            fFinished = true;
            return;
        }
        fRwl.writeLock().lock();
        try {
            boolean isIntersectingSample = Long.divideUnsigned((stateStartTime - fStart), fNPixels) + 1 <= Long.divideUnsigned((stateEndTime - fStart), fNPixels);
            // Add if interval intersects multiple of resolution
            if (!isIntersectingSample && !fIgnoreResolutionCutOff && stateStartTime != fStart) {
                return;
            }
            // Save interval if interval bigger than resolution
            List<ITmfStateInterval> intervalList = fIntervalMap.computeIfAbsent(quark, k -> new ArrayList<>(1));
            TileInterval interval = new TileInterval(stateStartTime, stateEndTime, quark, value);
            intervalList.add(interval);
            fSize += interval.getSizeOnDisk(isEveryIntervalContiguous);
            //to remove 3lines
            if (!fIgnoreResolutionCutOff) {
                fDuplicatedSize += interval.getSizeOnDisk(isEveryIntervalContiguous);
                fDuplicatedIntervals += 1;
            }
        } finally {
            fRwl.writeLock().unlock();
        }
    }
}
