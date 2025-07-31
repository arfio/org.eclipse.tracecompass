package org.eclipse.tracecompass.internal.statesystem.core.backend.historytiles.constantsize;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.eclipse.tracecompass.statesystem.core.ITmfStateSystemBuilder;

/**
 * @since 5.4
 *
 */
public class HistoryTileConstantSizeMetadata {

    /**
     * The magic number for this file format.
     */
    public static final int HISTORY_FILE_MAGIC_NUMBER = 0x05FFB121;
    /**
     * Magic number, file version, provider version, header size and config size
     */
    private static final int STATIC_HEADER_SIZE = 3 * Integer.BYTES + Long.BYTES;
    private static final int FILE_VERSION = 1;
    // private static final @NonNull Logger LOGGER =
    // TraceCompassLog.getLogger(HistoryTileConstantSizeConfig.class);

    private File fStateFile;
    private long fStart;
    private long fEnd;
    private long fSize = 0;
    private long fAttributeTreePosition;
    // private long fPreviousMetadataTile = -1;
    private int fProviderVersion;
    private Map<Integer, List<Long>> fTileEndTimeMap = new HashMap<>();
    private Map<Integer, List<Long>> fTilePositionMap = new HashMap<>();

    HistoryTileConstantSizeMetadata(File stateFile, int providerVersion, long startTime) {
        fStateFile = stateFile;
        fProviderVersion = providerVersion;
        fStart = startTime;
        fEnd = startTime;
    }

    HistoryTileConstantSizeMetadata(File existingStateFile, int providerVersion, FileChannel readChannel) throws IOException {
        fStateFile = existingStateFile;
        fProviderVersion = providerVersion;
        readHeader(readChannel);
        readMetadata(readChannel);
    }

    public long getStart() {
        return fStart;
    }

    public File getStateFile() {
        return fStateFile;
    }

    public int getProviderVersion() {
        return fProviderVersion;
    }

    public void setEnd(long end) {
        fEnd = end;
    }

    public long getEnd() {
        // TODO: Read end from statefile
        return fEnd;
    }

    /**
     * @param writeChannel
     *            TODO: Write metadata to file
     */
    public void addTile(FileChannel writeChannel, int detailLevel, long position, long endTime) {
        List<Long> positions = fTilePositionMap.computeIfAbsent(detailLevel, k -> new ArrayList<>());
        List<Long> endTimes = fTileEndTimeMap.computeIfAbsent(detailLevel, k -> new ArrayList<>());
        positions.add(position);
        endTimes.add(endTime);
        fSize += 1;
        if (fSize > 10000) {
            writeMetadataTile(writeChannel);
        }
    }

    public void writeHeader(FileChannel writeChannel) {
        ByteBuffer buffer = ByteBuffer.allocate(STATIC_HEADER_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.clear();

        try {
            writeChannel.position(0);

            /* Static header portion */
            buffer.putInt(HISTORY_FILE_MAGIC_NUMBER);
            buffer.putInt(FILE_VERSION);
            buffer.putInt(fProviderVersion);
            buffer.putLong(0L); // TODO: Calculate/get attribute tree position
            buffer.flip();
            writeChannel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readHeader(FileChannel readChannel) throws IOException {
        ByteBuffer headerBuffer = ByteBuffer.allocate(STATIC_HEADER_SIZE);
        headerBuffer.order(ByteOrder.LITTLE_ENDIAN);
        headerBuffer.clear();

        readChannel.position(0);
        readChannel.read(headerBuffer);

        int magicNumber = headerBuffer.getInt();
        if (magicNumber != HISTORY_FILE_MAGIC_NUMBER) {
            throw new IOException("Wrong magic number"); //$NON-NLS-1$
        }
        int fileVersion = headerBuffer.getInt();
        if (fileVersion != FILE_VERSION) {
            throw new IOException("Mismatching History Tile file format versions"); //$NON-NLS-1$
        }
        int providerVersion = headerBuffer.getInt();
        if (providerVersion != fProviderVersion && providerVersion != ITmfStateSystemBuilder.IGNORE_PROVIDER_VERSION) {
            throw new IOException("Mismatching event handler versions"); //$NON-NLS-1$
        }
        fAttributeTreePosition = headerBuffer.getLong();
    }

    public long getAttributeTreePosition() {
        return fAttributeTreePosition;
    }

    /**
     * Return last tile sequence number
     *
     * @param detailLevel
     *            The interval size to be queried
     * @return sequence number
     */
    public int getLatestSequenceNumber(int detailLevel) {
        return fTileEndTimeMap.getOrDefault(detailLevel, Collections.emptyList()).size();
    }

    /**
     * @param writeChannel
     *            TODO: Write metadata to file
     */
    private void writeMetadataTile(FileChannel writeChannel) {
        // TODO: write it to file
        System.out.println("todo: writing metadata with size: " + fSize);
    }

    /**
     * @param readChannel
     *            TODO: Read metadata from file
     */
    private static Map<Long, Long> readMetadata(FileChannel readChannel) {
        return new HashMap<>();
        // TODO: Write algorithm to read back all the metadata dumps across the
        // history file.
    }

    public int getSequenceNumber(int detailLevel, long timestamp) {
        if (!fTileEndTimeMap.containsKey(detailLevel) || !fTilePositionMap.containsKey(detailLevel)) {
            return -1;
        }
        List<Long> endTimes = fTileEndTimeMap.get(detailLevel);
        int index = Collections.binarySearch(endTimes, timestamp);

        if (index < 0) {
            index = -index - 1;
        }
        // No tile for this detail level contains the interval
        if (index >= Objects.requireNonNull(endTimes).size()) {
            return -1;
        }
        // If endtime is duplicated, we need to go back to the first sample
        while (index >= 0 && Objects.requireNonNull(endTimes).get(index) > timestamp) {
            index--;
        }
        return ++index;
    }

    public long getTilePosition(int detailLevel, int sequenceNumber) {
        if (sequenceNumber >= 0) {
            List<Long> tilePositions = fTilePositionMap.get(detailLevel);
            if (tilePositions != null && sequenceNumber < tilePositions.size()) {
                return tilePositions.get(sequenceNumber);
            }
        }
        return -1;
    }
}
