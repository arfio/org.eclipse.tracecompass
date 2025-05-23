package org.eclipse.tracecompass.internal.statesystem.core.backend.historytiles.constantsize;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.common.core.log.TraceCompassLog;
import org.eclipse.tracecompass.internal.statesystem.core.Activator;
import org.eclipse.tracecompass.statesystem.core.backend.IStateHistoryBackend;
import org.eclipse.tracecompass.statesystem.core.exceptions.StateSystemDisposedException;
import org.eclipse.tracecompass.statesystem.core.exceptions.TimeRangeException;
import org.eclipse.tracecompass.statesystem.core.interval.ITmfStateInterval;
import org.eclipse.tracecompass.traceeventlogger.LogUtils;

public class HistoryTileConstantSizeBackend implements IStateHistoryBackend {

    private static final @NonNull Logger LOGGER = TraceCompassLog.getLogger(HistoryTileConstantSizeBackend.class);

    private HistoryTileConstantSizeConfig fConfig;
    private final @NonNull String fSsid;
    private long fEnd;
    private int fTileSize;
    private boolean fFinishedBuilding = false;
    /* Fields related to the file I/O */
    private final FileInputStream fFileInputStream;
    private final FileOutputStream fFileOutputStream;
    private final FileChannel fReadChannel;
    private final FileChannel fWriteChannel;

    HistoryTileConstantSizeBackend(String ssid, int providerVersion, File newStateFile, long startTime, int tileSize) throws IOException {
        fConfig = new HistoryTileConstantSizeConfig(newStateFile, providerVersion, startTime);
        fSsid = ssid;
        fEnd = startTime;
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
//        seekToTileSection(fReadChannel);
//        seekToTileSection(fWriteChannel);
    }


    HistoryTileConstantSizeBackend(String ssid, int providerVersion, File existingStateFile) throws IOException {
        fConfig = new HistoryTileConstantSizeConfig(existingStateFile, providerVersion);
        fSsid = ssid;
        fEnd = fConfig.getEnd();
        fFileInputStream = new FileInputStream(existingStateFile);
        fFileOutputStream = new FileOutputStream(existingStateFile);
        fReadChannel = fFileInputStream.getChannel();
        fWriteChannel = fFileOutputStream.getChannel();
    }

    @Override
    public void insertPastState(long stateStartTime, long stateEndTime,
            int quark, Object value) throws TimeRangeException {

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
    public void finishedBuilding(long endTime) throws TimeRangeException {
        fEnd = endTime;
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
        } else {
            fConfig.getStateFile().delete();
        }
    }

    @Override
    public void doQuery(@NonNull List<@Nullable ITmfStateInterval> currentStateInfo, long t) throws TimeRangeException, StateSystemDisposedException {
        // TODO Auto-generated method stub

    }

    @Override
    public ITmfStateInterval doSingularQuery(long t, int attributeQuark) throws TimeRangeException, StateSystemDisposedException {
        // TODO Auto-generated method stub
        return null;
    }

}
