package org.eclipse.tracecompass.internal.statesystem.core.backend.historytiles;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.common.core.log.TraceCompassLog;
import org.eclipse.tracecompass.internal.datastore.core.condition.ContinuousTimeRangeCondition;
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
public class HistoryTileBackend implements IStateHistoryBackend {

    // TOREMOVE-------
    private List<Integer> fIntervalStatistics = new ArrayList<>();
    // --------
    private static final @NonNull Logger LOGGER = TraceCompassLog.getLogger(HistoryTileBackend.class);
    private HistoryTileConfig fConfig;
    private final @NonNull String fSsid;
    private long fEnd;
    private boolean fFinishedBuilding = false;
    private List<HistoryTile> fCachedTiles;
    /* Fields related to the file I/O */
    private final FileInputStream fFileInputStream;
    private final FileOutputStream fFileOutputStream;
    private final FileChannel fReadChannel;
    private final FileChannel fWriteChannel;

    public HistoryTileBackend(String ssid, File newStateFile,
            int providerVersion,
            long startTime, int nPixels, long[] resolutions, boolean isEveryIntervalContiguous) throws IOException {
        fConfig = new HistoryTileConfig(newStateFile, providerVersion, startTime, nPixels, resolutions, isEveryIntervalContiguous);
        fSsid = ssid;
        fEnd = startTime;
        fCachedTiles = new ArrayList<>(fConfig.getResolutions().length);

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
        seekToTileSection(fReadChannel);
        seekToTileSection(fWriteChannel);
    }

    public HistoryTileBackend(String ssid, File existingStateFile, int providerVersion) throws IOException {
        fConfig = new HistoryTileConfig(existingStateFile, providerVersion);
        fSsid = ssid;
        fEnd = fConfig.getEnd();
        fCachedTiles = new ArrayList<>(fConfig.getResolutions().length);
        fFileInputStream = new FileInputStream(existingStateFile);
        fFileOutputStream = new FileOutputStream(existingStateFile);
        fReadChannel = fFileInputStream.getChannel();
        fWriteChannel = fFileOutputStream.getChannel();
    }

    private void seekToTileSection(FileChannel channel) {
        try {
            channel.position(fConfig.getStartTileSection());
        } catch (IOException e) {
            Activator.getDefault().logError("Failed to seek in " + channel.toString()); //$NON-NLS-1$
        }
    }

    @Override
    public @NonNull String getSSID() {
        return fSsid;
    }

    @Override
    public long getStartTime() {
        return fConfig.getStart();
    }

    @Override
    public long getEndTime() {
        return fEnd;
    }

    @Override
    public void insertPastState(long stateStartTime, long stateEndTime,
            int quark, Object value) throws TimeRangeException {
        // TO REMOVE----------
        long stateDuration = stateEndTime - stateStartTime;
        int sizeIndex = (int) Math.log10(stateDuration);
        if (sizeIndex < 0) {
            sizeIndex = 0;
        }
        while (sizeIndex >= fIntervalStatistics.size()) {
            fIntervalStatistics.add(fIntervalStatistics.size(), 0);
        }
        fIntervalStatistics.set(sizeIndex, fIntervalStatistics.get(sizeIndex) + 1);
        // --------
        if (stateStartTime > stateEndTime) {
            throw new TimeRangeException("Start:" + stateStartTime + ", End:" + stateEndTime); //$NON-NLS-1$ //$NON-NLS-2$
        } else if (stateStartTime < fConfig.getStart()) {
            throw new TimeRangeException("Interval Start:" + stateStartTime + ", Config Start:" + fConfig.getStart()); //$NON-NLS-1$ //$NON-NLS-2$
        }

        fEnd = stateEndTime;
        for (int i = 0; i < fConfig.getResolutions().length; i++) {
            if (fCachedTiles.size() < i + 1) {
                // allocate new tile
                long endTimeTile = stateStartTime + fConfig.getNPixels() * fConfig.getResolutions()[i];
                fCachedTiles.add(new HistoryTile(fConfig.getResolutions()[i], stateStartTime, endTimeTile));
            }
            // insert interval in cached tile
            fCachedTiles.get(i).insertPastState(stateStartTime, stateEndTime, quark, value, fConfig.isEveryIntervalContiguous());
            if (fCachedTiles.get(i).isFinished()) {
                writeTileToDisk(fCachedTiles.get(i));
                // create new tile until we reach state start and insert
                // state
                HistoryTile nextHistoryTile = createNewTile(i, stateEndTime);
                fCachedTiles.set(i, nextHistoryTile);
                nextHistoryTile.insertPastState(stateStartTime, stateEndTime, quark, value, fConfig.isEveryIntervalContiguous());
            }
        }
    }

    @Override
    public void finishedBuilding(long endTime) throws TimeRangeException {
        fEnd = endTime;
        fCachedTiles.forEach(this::writeTileToDisk);
        // Finished building after writing all the tiles to avoid setting a tile in the cache at the same time.
        fFinishedBuilding = true;
        fConfig.writeHeader(fWriteChannel);
    }

    private HistoryTile createNewTile(int resolutionIndex, long endTime) {
        long startTile = fCachedTiles.get(resolutionIndex).getEnd() + 1;
        long endTimeTile = startTile + fConfig.getNPixels() * fConfig.getResolutions()[resolutionIndex];
        while (endTimeTile < endTime) {
            startTile = endTimeTile + 1;
            endTimeTile = startTile + fConfig.getNPixels() * fConfig.getResolutions()[resolutionIndex];
        }
        HistoryTile historyTile;
        if (resolutionIndex == fConfig.getResolutions().length - 1) {
            historyTile = new HistoryTile(fConfig.getResolutions()[resolutionIndex], startTile, endTimeTile, true);
        } else {
            historyTile = new HistoryTile(fConfig.getResolutions()[resolutionIndex], startTile, endTimeTile);
        }
        return historyTile;
    }

    private void writeTileToDisk(HistoryTile tile) {
        // write tile to disk
        try (FlowScopeLog next = new FlowScopeLogBuilder(LOGGER, Level.FINER, "HistoryTileBackend:writeTileToDisk").build()) { //$NON-NLS-1$
            long position = fWriteChannel.position();
            tile.writeSelf(fWriteChannel, fConfig.isEveryIntervalContiguous(), fSsid);
            // add tile to config
            fConfig.addTile(tile, position);
        } catch (IOException e) {
            /* If we were able to open the file, we should be fine now... */
            Activator.getDefault().logError(e.getMessage(), e);
        }
    }

    @Override
    public FileInputStream supplyAttributeTreeReader() {
        // seek to end of file.
        try {
            fReadChannel.position(supplyAttributeTreeWriterFilePosition());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("AttributeTree  reader supplied");
        return fFileInputStream;
    }

    @Override
    public File supplyAttributeTreeWriterFile() {
        System.out.println("AttributeTreeWrite writer supplied");
        return fConfig.getStateFile();
    }

    @Override
    public long supplyAttributeTreeWriterFilePosition() {
        System.out.println("AttributeTreeWrite position supplied");
        return fConfig.getStartTreeSection(fReadChannel);
    }

    @Override
    public void removeFiles() {
        File historyTreeFile = fConfig.getStateFile();
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
            LogUtils.traceInstant(LOGGER, Level.FINE, "HistoryTreeBackend:ClosingFile", "size", fConfig.getStateFile().length()); //$NON-NLS-1$ //$NON-NLS-2$
            LogUtils.traceObjectDestruction(LOGGER, Level.FINER, this);
            // TOREMOVE
            LogUtils.traceInstant(LOGGER, Level.FINE, "HistoryTreeBackend:ClosingFile", "ssid", fSsid, "statistics", fIntervalStatistics); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        } else {
            fConfig.getStateFile().delete();
        }
    }

    private HistoryTile readTile(int resolutionIndex, long time) {
        int tileIndex = 0;
        if (resolutionIndex > 0) {
            tileIndex = (int) Math.floorDiv(time - 1 - fConfig.getStart(), fConfig.getResolutions()[resolutionIndex] * fConfig.getNPixels());
            if (tileIndex < 0) {
                tileIndex = 0;
            }
        }
        if (resolutionIndex < fCachedTiles.size()) {
            HistoryTile tile = fCachedTiles.get(resolutionIndex);
            if (time >= tile.getStart() && time < tile.getEnd()) {
                return tile;
            }
        }
        HistoryTile tile = fConfig.readTile(fReadChannel, resolutionIndex, tileIndex);
        if (fFinishedBuilding) {
            fCachedTiles.set(resolutionIndex, tile);
        }
        return tile;
    }

    private int readBiggerResolution(int startingResolutionIndex) {
        long minimumIntervalSize = fConfig.getResolutions()[startingResolutionIndex] * fConfig.getNPixels();
        int resolutionIndex = startingResolutionIndex;
        while (resolutionIndex - 1 >= 0 && minimumIntervalSize > fConfig.getResolutions()[resolutionIndex - 1]) {
            resolutionIndex--;
        }
        return resolutionIndex;
    }

    @Override
    public void doQuery(@NonNull List<@Nullable ITmfStateInterval> currentStateInfo, long t) throws TimeRangeException, StateSystemDisposedException {
        for (int i = 0; i < currentStateInfo.size(); i++) {
            ITmfStateInterval interval = currentStateInfo.get(i);
            if (interval != null && !interval.intersects(t)) {
                currentStateInfo.set(i, null);
            }
        }
        int resolutionIndex = fConfig.getResolutions().length - 1;
        HistoryTile tile = readTile(resolutionIndex, t);
        tile.doQuery(currentStateInfo, t);
        while (currentStateInfo.contains(null) && resolutionIndex > 0) {
            // read adjacent tile
            long timeAdjacentTile = t + fConfig.getResolutions()[resolutionIndex] * fConfig.getNPixels();
            if (timeAdjacentTile <= fEnd) {
                tile = readTile(resolutionIndex, timeAdjacentTile);
                tile.doQuery(currentStateInfo, t);
            }
            if (!currentStateInfo.contains(null)) {
                return;
            }
            // read bigger resolution tile
            resolutionIndex = readBiggerResolution(resolutionIndex);
            tile = readTile(resolutionIndex, t);
            tile.doQuery(currentStateInfo, t);
        }
    }

    @Override
    public ITmfStateInterval doSingularQuery(long t, int attributeQuark) throws TimeRangeException, StateSystemDisposedException {
        if (t < fConfig.getStart() || t > fConfig.getEnd()) {
            throw new TimeRangeException(String.format("%s Time:%d, Start:%d, End:%d", //$NON-NLS-1$
                    fSsid, t, fConfig.getStart(), fConfig.getEnd()));
        }
        int resolutionIndex = fConfig.getResolutions().length - 1;
        HistoryTile tile = readTile(resolutionIndex, t);
        ITmfStateInterval interval = tile.doSingularQuery(t, attributeQuark);
        while (interval == null && resolutionIndex > 0) {
            // read adjacent tile
            tile = readTile(resolutionIndex, t + fConfig.getResolutions()[resolutionIndex] * fConfig.getNPixels());
            interval = tile.doSingularQuery(t, attributeQuark);
            if (interval != null) {
                return interval;
            }
            // read bigger resolution tile
            resolutionIndex = readBiggerResolution(resolutionIndex);
            tile = readTile(resolutionIndex, t);
            interval = tile.doSingularQuery(t, attributeQuark);
        }
        return interval;
    }

    @Override
    public Iterable<@NonNull ITmfStateInterval> query2D(IntegerRangeCondition quarks, TimeRangeCondition times) {
        long[] timeArray = times.getTimeArray();
        if (times instanceof ContinuousTimeRangeCondition) {
            return query2DContinuous(quarks, times);
        }
        if (timeArray.length < 2 || timeArray[0] >= fEnd) {
            return Collections.emptyList();
        }
        // calculate tile resolution
        long requestedResolution = timeArray[1] - timeArray[0];
        int resolutionIndex = 0;
        while (fConfig.getResolutions()[resolutionIndex] > requestedResolution && resolutionIndex + 1 < fConfig.getResolutions().length) {
            resolutionIndex++;
        }
        System.out.println("requestedResolution: " + requestedResolution + ", resolutionSelected: " + fConfig.getResolutions()[resolutionIndex]); //$NON-NLS-1$ //$NON-NLS-2$

        Iterable<@NonNull ITmfStateInterval> result = Collections.emptyList();
        try (FlowScopeLog next = new FlowScopeLogBuilder(LOGGER, Level.FINER, "HistoryTileBackend:initQuery2D").build()) { //$NON-NLS-1$
            HistoryTile tile = readTile(resolutionIndex, timeArray[0]);
            // query 2D every tile until end of trace or end of requested times
            result = tile.query2D(quarks, times);
            int i = 1;
            while (tile.getEnd() < times.max() && tile.getEnd() < fEnd) {
                tile = readTile(resolutionIndex, timeArray[0] + i * fConfig.getResolutions()[resolutionIndex] * fConfig.getNPixels());
                result = Iterables.concat(result, tile.query2D(quarks, times));
                i++;
            }
            // check for missing intervals in last tile
            Collection<@NonNull Integer> missingIntervalQuarks = tile.checkMissingInterval(quarks.getIntegerArray(), times.max());
            // if missed interval -> doquery on times.max()
            int nAttributes = fCachedTiles.get(0).getNumberAttributes();
            List<@Nullable ITmfStateInterval> currentStateInfo = new ArrayList<>(Collections.nCopies(nAttributes, null));
            doQuery(currentStateInfo, times.max());
            // add intervals to iterable for all intervals that are in the
            // returned integerrangecondition
            List<ITmfStateInterval> missingIntervals = new ArrayList<>();
            for (int missingIntervalQuark : missingIntervalQuarks) {
                if (missingIntervalQuark < currentStateInfo.size() && currentStateInfo.get(missingIntervalQuark) != null) {
                    missingIntervals.add(currentStateInfo.get(missingIntervalQuark));
                }
            }
            result = Iterables.concat(result, missingIntervals);
        } catch (TimeRangeException | StateSystemDisposedException e) {
            e.printStackTrace();
        }
        return result;
    }

    private Iterable<@NonNull ITmfStateInterval> query2DContinuous(IntegerRangeCondition quarks, TimeRangeCondition times) {
        Iterable<@NonNull ITmfStateInterval> result = Collections.emptyList();
        int resolutionIndex = fConfig.getResolutions().length - 1;
        try (FlowScopeLog next = new FlowScopeLogBuilder(LOGGER, Level.FINER, "HistoryTileBackend:initQuery2D").build()) { //$NON-NLS-1$
            long timeCursor = times.min();

            int nAttributes = quarks.max() + 1;
            List<@Nullable ITmfStateInterval> currentStateInfo = new ArrayList<>(Collections.nCopies(nAttributes, null));
            doQuery(currentStateInfo, times.max());
            // Get relevant intervals at end time.
            List<@NonNull ITmfStateInterval> endIntervals = new ArrayList<>();
            for (int quark: quarks.getIntegerArray()) {
                ITmfStateInterval interval = currentStateInfo.get(quark);
                if (interval != null) {
                    timeCursor = Long.max(interval.getStartTime(), timeCursor);
                    // if the tile is the same, this interval will be added at the second step
                    int tileIndexRequestEnd = (int) Math.floorDiv(times.max() - 1 - fConfig.getStart(), fConfig.getResolutions()[resolutionIndex] * fConfig.getNPixels());
                    int tileIndexInterval = (int) Math.floorDiv(interval.getEndTime() - 1 - fConfig.getStart(), fConfig.getResolutions()[resolutionIndex] * fConfig.getNPixels());
                    if (tileIndexRequestEnd != tileIndexInterval) {
                        endIntervals.add(interval);
                    }
                }
            }
            // Go from the end to the start time, getting all the required intervals.
            result = Iterables.concat(result, endIntervals);
            while (timeCursor >= times.min()) {
                // Read farthest previous tile
                HistoryTile tile = readTile(resolutionIndex, timeCursor);
                Iterable<@NonNull ITmfStateInterval> intervals = tile.query2D(quarks, times);
                // Go to end of smallest interval for all the quarks
                timeCursor = times.min();
                for (ITmfStateInterval interval : intervals) {
                    timeCursor = Long.max(interval.getStartTime(), timeCursor);
                }
                timeCursor = Long.min(tile.getStart() - 1, timeCursor);
                result = Iterables.concat(result, intervals);
            }
            return result;
        } catch (TimeRangeException e) {
            e.printStackTrace();
        } catch (StateSystemDisposedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }
}
