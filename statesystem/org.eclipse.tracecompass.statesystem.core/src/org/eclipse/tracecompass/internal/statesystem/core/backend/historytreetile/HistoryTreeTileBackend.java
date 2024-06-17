package org.eclipse.tracecompass.internal.statesystem.core.backend.historytreetile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.internal.statesystem.core.Activator;
import org.eclipse.tracecompass.internal.statesystem.core.backend.historytree.ThreadedHistoryTreeBackend;
import org.eclipse.tracecompass.statesystem.core.backend.IStateHistoryBackend;
import org.eclipse.tracecompass.statesystem.core.exceptions.StateSystemDisposedException;
import org.eclipse.tracecompass.statesystem.core.exceptions.TimeRangeException;
import org.eclipse.tracecompass.statesystem.core.interval.ITmfStateInterval;

public class HistoryTreeTileBackend implements IStateHistoryBackend {

    private Map<Integer, ThreadedHistoryTreeBackend> fHistoryTreeTiles = new HashMap<>();

    private final String fSsid;
    private final long fStartTime;
    private final int fProviderVersion;
    private long fEndTime;

    HistoryTreeTileBackend(String ssid, long startTime, int providerVersion) {
        fSsid = ssid;
        fStartTime = startTime;
        fEndTime = startTime;
        fProviderVersion = providerVersion;
    }

    @Override
    public @NonNull String getSSID() {
        return fSsid;
    }

    @Override
    public long getStartTime() {
        return fStartTime;
    }

    @Override
    public long getEndTime() {
        return fEndTime;
    }

    @Override
    public void finishedBuilding(long endTime) throws TimeRangeException {
        for (ThreadedHistoryTreeBackend historyTreeBackend : fHistoryTreeTiles.values()) {
            historyTreeBackend.finishedBuilding(endTime);
        }
    }

    @Override
    public FileInputStream supplyAttributeTreeReader() {
        Optional<ThreadedHistoryTreeBackend> historyTree = fHistoryTreeTiles.values().stream().findAny();
        if (historyTree.isPresent()) {
            try (FileInputStream atReader = historyTree.get().supplyAttributeTreeReader()) {
                return Objects.requireNonNull(atReader);
            } catch (IOException e) {
                Activator.getDefault().logError(e.getMessage(), e);
            }
        }
        resetHistoryTreeTiles();
        return supplyAttributeTreeReader();
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

    private void resetHistoryTreeTiles() {
        for (ThreadedHistoryTreeBackend historyTreeBackend : fHistoryTreeTiles.values()) {
            historyTreeBackend.dispose();
        }
        int numberOfTiles = Long.numberOfLeadingZeros(fEndTime - fStartTime);
        numberOfTiles = numberOfTiles == 0 ? 1 : numberOfTiles;
        for (int i = 0; i < numberOfTiles; i++) {
            try {
                ThreadedHistoryTreeBackend tile = new ThreadedHistoryTreeBackend(fSsid, null, fProviderVersion, fStartTime, 10000);
                fHistoryTreeTiles.put(i, tile);
            } catch (IOException e) {
                Activator.getDefault().logError(e.getMessage(), e);
            }
        }
    }

    @Override
    public void removeFiles() {
        // TODO Auto-generated method stub

    }

    @Override
    public void dispose() {
        // TODO Auto-generated method stub

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
