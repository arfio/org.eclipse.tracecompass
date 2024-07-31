package org.eclipse.tracecompass.internal.statesystem.core.backend.historytiles;

import java.io.IOException;
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

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.datastore.core.encoding.HTVarInt;
import org.eclipse.tracecompass.internal.provisional.datastore.core.condition.IntegerRangeCondition;
import org.eclipse.tracecompass.internal.provisional.datastore.core.condition.TimeRangeCondition;
import org.eclipse.tracecompass.statesystem.core.interval.ITmfStateInterval;


/**
 * @since 5.4
 *
 */
public class HistoryTile {

    private Map<Integer, List<@NonNull ITmfStateInterval>> fIntervalMap = new HashMap<>();
    private long fResolution;
    private long fStart;
    private long fEnd;
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

    HistoryTile(long resolution, long start, long end, boolean ignoreResolutionCutOff) {
        this(resolution, start, end);
        fIgnoreResolutionCutOff = ignoreResolutionCutOff;
    }

    HistoryTile(long resolution, long start, long end, Map<Integer, List<@NonNull ITmfStateInterval>> intervalMap) {
        this(resolution, start, end);
        fIntervalMap = intervalMap;
    }

    public void writeSelf(FileChannel channel) {
        fRwl.readLock().lock();
        try {
            int nAttributes = fIntervalMap.size();
            int tileSize = (nAttributes * 2 + 2) * Integer.BYTES + fSize;
            for (List<@NonNull ITmfStateInterval> intervalList: fIntervalMap.values()) {
                tileSize += HTVarInt.getEncodedLengthLong(intervalList.get(0).getStartTime());
            }
            ByteBuffer buffer = ByteBuffer.allocate(tileSize);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.clear();
            buffer.putInt(tileSize);
            buffer.putInt(nAttributes);
            for (Entry<Integer, List<@NonNull ITmfStateInterval>> entry: fIntervalMap.entrySet()) {
                List<@NonNull ITmfStateInterval> intervalList = entry.getValue();
                buffer.putInt(intervalList.size());
                buffer.putInt(entry.getKey());
                HTVarInt.writeLong(buffer, intervalList.get(0).getStartTime());
                for (ITmfStateInterval interval: intervalList) {
                    ((TileInterval) interval).writeInterval(buffer);
                }
            }

            /* Finally, write everything in the Buffer to disk */
            buffer.flip();
            int res = channel.write(buffer);
            if (res != tileSize) {
                throw new IllegalStateException("Wrong size of block written: Actual: " + res + ", Expected: " + tileSize); //$NON-NLS-1$ //$NON-NLS-2$
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            fRwl.readLock().unlock();
        }
    }

    public static HistoryTile readTile(FileChannel channel, long position, int tileSize, long resolution, long start, long end) {
        Map<Integer, List<@NonNull ITmfStateInterval>> intervalMap = new HashMap<>();
        ByteBuffer buffer = ByteBuffer.allocate(tileSize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.clear();
        try {
            channel.position(position);
            channel.read(buffer);
            buffer.flip();
            buffer.getInt(); // skip tileSize
            int nAttributes = buffer.getInt();

            for (int i = 0; i < nAttributes; i++) {
                int intervalListSize = buffer.getInt();
                int attributeQuark = buffer.getInt();
                long startTime = HTVarInt.readLong(buffer);
                List<@NonNull ITmfStateInterval> intervalList = new ArrayList<>(intervalListSize);
                for (int j = 0; j < intervalListSize; j++) {
                    TileInterval interval = TileInterval.readInterval(buffer, startTime, attributeQuark);
                    intervalList.add(interval);
                    startTime += interval.getEndTime() - interval.getStartTime();
                }
                intervalMap.put(attributeQuark, intervalList);
            }


        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return new HistoryTile(resolution, start, end, intervalMap);
    }

    public String debugInfo() {
        int size = 0;
        for (Entry<Integer, List<@NonNull ITmfStateInterval>> entry: fIntervalMap.entrySet()) {
            size += entry.getValue().size();
        }
        return "resolution: " + fResolution + ", interval map size: " + size;
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
        List<ITmfStateInterval> intervalList = fIntervalMap.getOrDefault(attributeQuark, Collections.emptyList());
        for (ITmfStateInterval interval : intervalList) {
            if (t >= interval.getStartTime() && t <= interval.getEndTime()) {
                return interval;
            }
        }
        return null;
    }

    public Collection<@NonNull Integer> checkMissingInterval(int[] quarks, long time) {
        Collection<@NonNull Integer> missingIntervalQuarks = new ArrayList<>();
        for (int i = 0; i < quarks.length; i++) {
            List<@NonNull ITmfStateInterval> intervalList = fIntervalMap.getOrDefault(quarks[i], null);
            if (intervalList == null || intervalList.get(intervalList.size() - 1).getEndTime() < time) {
                missingIntervalQuarks.add(quarks[i]);
            }
        }
        return missingIntervalQuarks;
    }

    public Iterable<@NonNull ITmfStateInterval> query2D(IntegerRangeCondition quarks, TimeRangeCondition times) {
        return () -> fIntervalMap
                .entrySet()
                .stream()
                .filter(e -> quarks.test(e.getKey()))
                .map(Entry::getValue)
                .flatMap(List::stream)
                .filter(i -> times.intersects(i.getStartTime(), i.getEndTime()))
                .iterator();
    }

    public void insertPastState(long stateStartTime, long stateEndTime,
            int quark, Object value) {
        if (stateEndTime < fStart) {
            return; // Ignore any interval outside the tile range
        }
        if (stateEndTime > fEnd) {
            fFinished = true;
            return;
        }
        // Save interval if interval bigger than resolution
        List<ITmfStateInterval> intervalList = fIntervalMap.computeIfAbsent(quark, k -> new ArrayList<>(1));
        // Interval smaller than resolution -> Add while previous interval < resolution
        if (stateEndTime - stateStartTime < fResolution && !intervalList.isEmpty() && !fIgnoreResolutionCutOff) {
            TileInterval lastInterval = (TileInterval) intervalList.get(intervalList.size() - 1);
            if (lastInterval.getEndTime() - lastInterval.getStartTime() < fResolution && !lastInterval.isNull()) {
                fSize += HTVarInt.getEncodedLengthLong(stateEndTime - lastInterval.getStartTime()) - HTVarInt.getEncodedLengthLong(lastInterval.getEndTime() - lastInterval.getStartTime());
                lastInterval.setEndTime(stateEndTime);
                return;
            }
        }
        TileInterval interval = new TileInterval(stateStartTime, stateEndTime, quark, value);
        intervalList.add(interval);
        fSize += interval.getSizeOnDisk();
    }
}
