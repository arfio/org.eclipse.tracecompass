package org.eclipse.tracecompass.internal.statesystem.core.backend.historytiles.constantsize;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.common.core.log.TraceCompassLog;
import org.eclipse.tracecompass.internal.provisional.datastore.core.condition.IntegerRangeCondition;
import org.eclipse.tracecompass.internal.provisional.datastore.core.condition.TimeRangeCondition;
import org.eclipse.tracecompass.internal.statesystem.core.Activator;
import org.eclipse.tracecompass.statesystem.core.backend.IStateHistoryBackend;
import org.eclipse.tracecompass.statesystem.core.exceptions.StateSystemDisposedException;
import org.eclipse.tracecompass.statesystem.core.exceptions.TimeRangeException;
import org.eclipse.tracecompass.statesystem.core.interval.ITmfStateInterval;
import org.eclipse.tracecompass.traceeventlogger.LogUtils;
import org.eclipse.tracecompass.traceeventlogger.LogUtils.FlowScopeLog;
import org.eclipse.tracecompass.traceeventlogger.LogUtils.FlowScopeLogBuilder;

import com.google.common.collect.Iterables;

/**
 * @since 5.4
 *
 */
public class HistoryTileConstantSizeBackend implements IStateHistoryBackend {

    private static final @NonNull Logger LOGGER = TraceCompassLog.getLogger(HistoryTileConstantSizeBackend.class);

    private HistoryTileConstantSizeMetadata fMetadata;
    private final @NonNull String fSsid;
    private int fTileSize;
    private boolean fFinishedBuilding = false;
    /* Fields related to the file I/O */
    private final FileInputStream fFileInputStream;
    private final FileOutputStream fFileOutputStream;
    private final FileChannel fReadChannel;
    private final FileChannel fWriteChannel;
    private List<HistoryTileConstantSize> fCachedTiles = new ArrayList<>();

    HistoryTileConstantSizeBackend(String ssid, int providerVersion, File newStateFile, long startTime, int tileSize) throws IOException {
        fMetadata = new HistoryTileConstantSizeMetadata(newStateFile, providerVersion, startTime);
        fSsid = ssid;
        fTileSize = tileSize;

        if (newStateFile.exists()) {
            Files.delete(newStateFile.toPath());
            /* delete can fail as long as file no longer exists */
            if (newStateFile.exists()) {
                throw new IOException("Cannot delete existing file at " + //$NON-NLS-1$
                        newStateFile.getName());
            }
        }
        if (!(newStateFile.createNewFile())) {
            /* It seems we do not have permission to create the new file */
            throw new IOException("Cannot create new file at " + //$NON-NLS-1$
                    newStateFile.getName());
        }
        fFileInputStream = new FileInputStream(newStateFile);
        fFileOutputStream = new FileOutputStream(newStateFile);
        fReadChannel = fFileInputStream.getChannel();
        fWriteChannel = fFileOutputStream.getChannel();
        // seekToTileSection(fReadChannel);
        // seekToTileSection(fWriteChannel);
    }

    HistoryTileConstantSizeBackend(String ssid, int providerVersion, File existingStateFile) throws IOException {
        fSsid = ssid;
        fFileInputStream = new FileInputStream(existingStateFile);
        fFileOutputStream = new FileOutputStream(existingStateFile);
        fReadChannel = fFileInputStream.getChannel();
        fWriteChannel = fFileOutputStream.getChannel();
        fMetadata = new HistoryTileConstantSizeMetadata(existingStateFile, providerVersion, fReadChannel);
    }

    @Override
    public void insertPastState(long stateStartTime, long stateEndTime,
            int quark, Object value) throws TimeRangeException {
        if (stateStartTime > stateEndTime) {
            throw new TimeRangeException("Start:" + stateStartTime + ", End:" + stateEndTime); //$NON-NLS-1$ //$NON-NLS-2$
        } else if (stateStartTime < fMetadata.getStart()) {
            throw new TimeRangeException("Interval Start:" + stateStartTime + ", Config Start:" + fMetadata.getStart()); //$NON-NLS-1$ //$NON-NLS-2$
        }
        fMetadata.setEnd(stateEndTime);

        // Select correct level
        int detailLevel = (int) Math.log10(stateEndTime - (double) stateStartTime);
        if (detailLevel < 0) {
            detailLevel = 0;
        }
        // if no tile then create tile at level
        while (detailLevel >= fCachedTiles.size()) {
            fCachedTiles.add(null);
        }
        if (fCachedTiles.get(detailLevel) == null) {
            fCachedTiles.set(detailLevel, new HistoryTileConstantSize(stateStartTime, fTileSize, fMetadata.getLatestSequenceNumber(detailLevel)));
        }
        HistoryTileConstantSize tile = fCachedTiles.get(detailLevel);
        // Insert interval
        tile.insertInterval(stateStartTime, stateEndTime, quark, value);
        // if complete write to disk
        if (tile.isFinished()) {
            writeTileToDisk(tile, detailLevel);
            LOGGER.info("Writing tile to disk");
            fCachedTiles.set(detailLevel, null);
        }
    }

    private void writeTileToDisk(HistoryTileConstantSize tile, int detailLevel) {
        try {
            long position = fWriteChannel.position();
            tile.writeSelf(fWriteChannel);
            fMetadata.addTile(fWriteChannel, detailLevel, position, tile.getEnd());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int getTileSize(long tilePosition) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.clear();
        try {
            synchronized (fReadChannel) {
                fReadChannel.position(tilePosition);
                fReadChannel.read(buffer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        buffer.flip();
        return buffer.getInt();
    }

    /**
     * Returns the tile that contains the given timestamp
     *
     * @param detailLevel
     * @param timestamp
     * @return
     */
    private HistoryTileConstantSize readTileFromDisk(int detailLevel, long timestamp) {
        int sequenceNumber = fMetadata.getSequenceNumber(detailLevel, timestamp);
        long tilePosition = fMetadata.getTilePosition(detailLevel, sequenceNumber);
        if (tilePosition < 0) {
            return null;
        }
        try (FlowScopeLog next = new FlowScopeLogBuilder(LOGGER, Level.FINER,
                "HistoryTileConstantSizeBackend:readTileFromDisk").build()) { //$NON-NLS-1$
            int tileSize = getTileSize(tilePosition);
            ByteBuffer buffer = ByteBuffer.allocate(tileSize);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.clear();
            synchronized (fReadChannel) {
                fReadChannel.position(tilePosition);
                fReadChannel.read(buffer);
            }
            return HistoryTileConstantSize.readTile(buffer, fTileSize, sequenceNumber);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private HistoryTileConstantSize readTileFromDisk(int detailLevel, int sequenceNumber) {
        long tilePosition = fMetadata.getTilePosition(detailLevel, sequenceNumber);
        if (tilePosition < 0) {
            return null;
        }
        try (FlowScopeLog next = new FlowScopeLogBuilder(LOGGER, Level.FINER,
                "HistoryTileConstantSizeBackend:readTileFromDisk").build()) { //$NON-NLS-1$
            int tileSize = getTileSize(tilePosition);
            ByteBuffer buffer = ByteBuffer.allocate(tileSize);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.clear();
            synchronized (fReadChannel) {
                fReadChannel.position(tilePosition);
                fReadChannel.read(buffer);
            }
            return HistoryTileConstantSize.readTile(buffer, fTileSize, sequenceNumber);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public @NonNull String getSSID() {
        return fSsid;
    }

    @Override
    public long getStartTime() {
        return fMetadata.getStart();
    }

    @Override
    public long getEndTime() {
        return fMetadata.getEnd();
    }

    @Override
    public void finishedBuilding(long endTime) throws TimeRangeException {
        fMetadata.setEnd(endTime);
        for (int i = 0; i < fCachedTiles.size(); i++) {
            HistoryTileConstantSize tile = fCachedTiles.get(i);
            if (tile != null) {
                writeTileToDisk(tile, i);
                tile.setFinished();
            }
        }
        // TODO: Write last intervals
        // TODO: Write config to disk
        fFinishedBuilding = true;
    }

    @Override
    public FileInputStream supplyAttributeTreeReader() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public File supplyAttributeTreeWriterFile() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long supplyAttributeTreeWriterFilePosition() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void removeFiles() {
        File historyTreeFile = fMetadata.getStateFile();
        try {
            Files.delete(historyTreeFile.toPath());
        } catch (IOException e) {
            Activator.getDefault().logError(e.getMessage(), e);
        }
    }

    @Override
    public void dispose() {
        try {
            fFileInputStream.close();
            fFileOutputStream.close();
        } catch (IOException e) {
            Activator.getDefault().logError(e.getMessage(), e);
        }
        if (fFinishedBuilding) {
            LogUtils.traceInstant(LOGGER, Level.FINE, "HistoryTreeBackend:ClosingFile", "size", fMetadata.getStateFile().length()); //$NON-NLS-1$ //$NON-NLS-2$
            LogUtils.traceObjectDestruction(LOGGER, Level.FINER, this);
        } else {
            fMetadata.getStateFile().delete();
        }
    }

    @Override
    public void doQuery(@NonNull List<@Nullable ITmfStateInterval> currentStateInfo, long t) throws TimeRangeException, StateSystemDisposedException {
        // Read tile at every detail level and doQuery on each
        for (int i = 0; i < fCachedTiles.size(); i++) {
            HistoryTileConstantSize tile = readTileFromDisk(i, t);
            if (tile != null) {
                tile.doQuery(currentStateInfo, t);
            }
        }
    }

    @Override
    public ITmfStateInterval doSingularQuery(long t, int attributeQuark) throws TimeRangeException, StateSystemDisposedException {
        if (t < fMetadata.getStart() || t > fMetadata.getEnd()) {
            throw new TimeRangeException(String.format("%s Time:%d, Start:%d, End:%d", //$NON-NLS-1$
                    fSsid, t, fMetadata.getStart(), fMetadata.getEnd()));
        }
        /*
         * Finding the interval for sure requires reading all the tiles between
         * t and t + 10 ^ detailLevel. If the interval is bigger then it will be
         * on a higher detail level.
         */
        for (int i = 0; i < fCachedTiles.size(); i++) {
            // Trying out the cached tile
            HistoryTileConstantSize cachedTile = fCachedTiles.get(i);
            if (cachedTile != null && cachedTile.getStart() <= t && cachedTile.getEnd() >= t) {
                ITmfStateInterval interval = cachedTile.doSingularQuery(t, attributeQuark);
                if (interval != null) {
                    return interval;
                }
            }
            // Read all the tiles until the interval size is too big to be located at that detail level.
            HistoryTileConstantSize tile = readTileFromDisk(i, t);
            while (tile != null && t + Math.pow(10, i) > tile.getStart()) {
                System.out.println("Interval searched at level: " + i + " sequence number: " + tile.getSequenceNumber());
                ITmfStateInterval interval = tile.doSingularQuery(t, attributeQuark);
                if (interval != null) {
                    return interval;
                }
                tile = readTileFromDisk(i, tile.getSequenceNumber() + 1);
            }
        }
        bruteforceLocation(t, attributeQuark);
        System.out.println("SingularQuery fail with t: " + t + " , quark: " + attributeQuark);
        return null;
    }

    private void bruteforceLocation(long t, int attributeQuark) {
        for (int i = 0; i < fCachedTiles.size(); i++) {
            for (int j = 0; j < fMetadata.getLatestSequenceNumber(i); j++) {

                HistoryTileConstantSize tile = readTileFromDisk(i, j);
                if (tile == null) {
                    tile = readTileFromDisk(i, j);
                    System.out.println("tile not found ? level: " + i + " sequence number: " + j);
                    continue;
                }
                ITmfStateInterval interval = tile.doSingularQuery(t, attributeQuark);
                if (interval != null) {
                    System.out.println("Interval found at level: " + i + " sequence number: " + j);
                }
            }
        }
    }

    @Override
    public Iterable<@NonNull ITmfStateInterval> query2D(IntegerRangeCondition quarks, TimeRangeCondition times) {
        // Calculate lowest level
        long[] timeArray = times.getTimeArray();
        if (timeArray.length <= 2 || fCachedTiles.isEmpty()) {
            return Collections.emptyList();
        }

        System.out.println("query2d constant size");

        double distanceBetweenTimes = timeArray[1] - (double) timeArray[0];
        int lowestDetailLevel = (int) Math.log10(distanceBetweenTimes);
        if (lowestDetailLevel < 0) {
            lowestDetailLevel = 0;
        }
        int highDetailLevel = (int) Math.log10(times.max() - times.min());

        Iterable<@NonNull ITmfStateInterval> result = Collections.emptyList();

        try (FlowScopeLog next = new FlowScopeLogBuilder(LOGGER, Level.FINER,
                "HistoryTileConstantSizeBackend:query2DInitialisation").build()) { //$NON-NLS-1$
            // Get large intervals
            for (int i = fCachedTiles.size() - 1; i > highDetailLevel; i--) {
                HistoryTileConstantSize tile = readTileFromDisk(i, times.min());
                /*
                 * If the cached tile is the first tile for this detail level, then
                 * no tile has been written to disk for this detaillevel. Therefore,
                 * the tileposition will be -1 and in this case, we need to return
                 * the cached tile.
                 */
                tile = tile == null ? fCachedTiles.get(i) : tile;
                if (tile != null) {
                    result = Iterables.concat(result, tile.query2d(quarks, times));
                }
            }

            // Fill in the rest
            for (int i = Math.min(highDetailLevel, fCachedTiles.size() - 1); i >= lowestDetailLevel; i--) {
                HistoryTileConstantSize tile = readTileFromDisk(i, times.min());
                // Same comment as the previous loop
                tile = tile == null ? fCachedTiles.get(i) : tile;
                while (tile != null) {
                    result = Iterables.concat(result, tile.query2d(quarks, times));
                    if (tile.getEnd() > times.max()) {
                        break;
                    }
                    tile = readTileFromDisk(i, tile.getSequenceNumber() + 1);
                }
            }
        }
        return result;
    }
}
