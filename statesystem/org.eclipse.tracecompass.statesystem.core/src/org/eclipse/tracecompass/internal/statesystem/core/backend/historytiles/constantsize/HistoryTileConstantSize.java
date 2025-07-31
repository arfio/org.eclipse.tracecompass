package org.eclipse.tracecompass.internal.statesystem.core.backend.historytiles.constantsize;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.datastore.core.encoding.HTVarInt;
import org.eclipse.tracecompass.internal.provisional.datastore.core.condition.IntegerRangeCondition;
import org.eclipse.tracecompass.internal.provisional.datastore.core.condition.TimeRangeCondition;
import org.eclipse.tracecompass.statesystem.core.interval.ITmfStateInterval;
import org.eclipse.tracecompass.statesystem.core.interval.TmfStateInterval;

/**
 * @since 5.4
 *
 */
public class HistoryTileConstantSize {

    /** Maximum number of intervals */
    private final int fMaxSize;
    private final int fSequenceNumber;
    private long fStart;
    private long fEnd = -1;
    private long[] fEndTimes;
    private List<PartialInterval> fIntervalData;
    private boolean fFinished = false;

    public HistoryTileConstantSize(long start, int maxSize, int sequenceNumber) {
        fSequenceNumber = sequenceNumber;
        fMaxSize = maxSize;
        fIntervalData = new ArrayList<>();
        fEndTimes = new long[fMaxSize];
        fStart = start;
    }

    public HistoryTileConstantSize(long start, long end, int sequenceNumber, long[] endTimes, List<PartialInterval> intervalData) {
        fSequenceNumber = sequenceNumber;
        fMaxSize = endTimes.length;
        fIntervalData = intervalData;
        fEndTimes = endTimes;
        fStart = start;
        fEnd = end;
        fFinished = true;
    }

    public void insertInterval(long start, long end, int quark, Object value) {
        if (fFinished) {
            return;
        }
        if (start < fStart) {
            fStart = start;
        }
        fEndTimes[fIntervalData.size()] = end;
        PartialInterval intervalData = new PartialInterval(start, value, quark);
        fIntervalData.add(intervalData);
        fEnd = Math.max(end, fEnd);
        if (fIntervalData.size() >= fMaxSize) {
            fFinished = true;
        }
    }

    public boolean isFinished() {
        return fFinished;
    }

    public void setFinished() {
        fFinished = true;
    }

    public long getStart() {
        return fStart;
    }

    public long getEnd() {
        return fEnd;
    }

    public int getSequenceNumber() {
        return fSequenceNumber;
    }

    private ITmfStateInterval createStateInterval(int index) {
        // change with offset and buffer to avoid having to parse all interval
        // data
        PartialInterval partialInterval = fIntervalData.get(index);
        return new TmfStateInterval(partialInterval.getStart(), fEndTimes[index], partialInterval.getAttribute(), partialInterval.getValue());
    }

    public Iterable<ITmfStateInterval> query2d(IntegerRangeCondition quarks, TimeRangeCondition times) {
//        System.out.println("query2d on tile with start " + fStart + " and end " + fEnd);
        if (fFinished) {
            return () -> IntStream.range(0, fIntervalData.size())
                    .filter(i -> times.intersects(fStart, fEndTimes[i]))
                    .mapToObj(this::createStateInterval)
                    .filter(i -> quarks.test(i.getAttribute()))
                    .iterator();
        }
        return Collections.emptyList();
    }

    public void doQuery(@NonNull List<@Nullable ITmfStateInterval> currentStateInfo, long t) {
        if (t < fStart || t > fEndTimes[fIntervalData.size() - 1]) {
            return;
        }
        int index = Arrays.binarySearch(fEndTimes, 0, fIntervalData.size() - 1, t);
        if (index < 0) {
            index = -index - 1;
        }
        long start = fEndTimes[index];
        while (start <= t && index < fIntervalData.size()) {
            ITmfStateInterval interval = createStateInterval(index);
            if (interval.getEndTime() >= t && interval.getAttribute() < currentStateInfo.size()) {
//                currentStateInfo.set(interval.getAttribute(), interval);
            }
            index++;
            if (index >= fMaxSize) {
                break;
            }
            start = fEndTimes[index];
        }
        for (int i = 0; i < fIntervalData.size(); i++) {
            if (t >= fIntervalData.get(i).getStart() && t <= fEndTimes[i]) {
                ITmfStateInterval interval = createStateInterval(i);
                if (interval.getAttribute() < currentStateInfo.size()) {
                    currentStateInfo.set(interval.getAttribute(), interval);
                }
            }
        }
    }

    public ITmfStateInterval doSingularQuery(long t, int attribute) {
        if (t < fStart || t > fEndTimes[fIntervalData.size() - 1]) {
            return null;
        }
        int index = Arrays.binarySearch(fEndTimes, 0, fIntervalData.size() - 1, t);
        if (index < 0) {
            index = -index - 1;
        }
        long end = fEndTimes[index];
        // Going backward in case of duplicated times
        if (index > 0) {
            while (index >= 0 && end == fEndTimes[index]) {
                index--;
            }
            index++;
        }
        // Checking each interval until the end time is after the query time
        while (end >= t) {
            ITmfStateInterval interval = createStateInterval(index);
            if (interval.getStartTime() <= t && interval.getAttribute() == attribute) {
                return interval;
            }
            index++;
            if (index >= fMaxSize) {
                break;
            }
            end = fEndTimes[index];
        }
        return null;
    }

    private int calculateTileSize() {
        // TOCHANGE
        int size = 0;
        size += 2 * Integer.BYTES; // tile size + number of intervals
        size += HTVarInt.getEncodedLengthLong(fStart);
        size += HTVarInt.getEncodedLengthLong(fEnd);
        for (int i = 0; i < fIntervalData.size(); i++) {
            PartialInterval pi = fIntervalData.get(i);
            long end = fEndTimes[i];
            size += HTVarInt.getEncodedLengthLong(end - fStart);
            size += HTVarInt.getEncodedLengthLong(pi.getStart() - fStart);
            size += Integer.BYTES; // attribute
            size += Byte.BYTES; // state type
            size += pi.getStateSize();
        }
        return size;
    }

    public void writeSelf(FileChannel channel) {
        try {
            int tileSize = calculateTileSize();
            ByteBuffer buffer = ByteBuffer.allocate(tileSize);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.clear();
            buffer.putInt(tileSize);
            buffer.putInt(fIntervalData.size());
            HTVarInt.writeLong(buffer, fStart);
            HTVarInt.writeLong(buffer, fEnd);

            for (int i = 0; i < fIntervalData.size(); i++) {
                HTVarInt.writeLong(buffer, fEndTimes[i] - fStart);
            }

            for (PartialInterval interval : fIntervalData) {
                HTVarInt.writeLong(buffer, interval.getStart() - fStart);
                buffer.putInt(interval.getAttribute());
                interval.writeStateValue(buffer);
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

    public static HistoryTileConstantSize readTile(ByteBuffer buffer, int maxSize, int sequenceNumber) {
        try {
            buffer.flip();
            buffer.getInt(); // skip tileSize
            int nInterval = buffer.getInt();
            long[] endTimes = new long[maxSize];
            long start = HTVarInt.readLong(buffer);
            long end = HTVarInt.readLong(buffer);
            for (int i = 0; i < nInterval; i++) {
                endTimes[i] = start + HTVarInt.readLong(buffer);
            }
            List<PartialInterval> partialIntervals = new ArrayList(nInterval);
            for (int i = 0; i < nInterval; i++) {
                long intervalEnd = start + HTVarInt.readLong(buffer);
                int attribute = buffer.getInt();
                Object stateValue = PartialInterval.readState(buffer);
                PartialInterval partialInterval = new PartialInterval(intervalEnd, stateValue, attribute);
                partialIntervals.add(partialInterval);
            }
            return new HistoryTileConstantSize(start, end, sequenceNumber, endTimes, partialIntervals);
        } catch (BufferUnderflowException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
