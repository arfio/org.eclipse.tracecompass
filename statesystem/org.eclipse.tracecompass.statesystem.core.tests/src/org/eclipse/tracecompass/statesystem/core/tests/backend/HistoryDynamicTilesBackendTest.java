package org.eclipse.tracecompass.statesystem.core.tests.backend;

import java.io.File;
import java.io.IOException;

import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.common.core.NonNullUtils;
import org.eclipse.tracecompass.internal.statesystem.core.backend.historytiles.dynamic.HistoryTileBackendDynamicFactory;
import org.eclipse.tracecompass.statesystem.core.backend.IStateHistoryBackend;
import org.junit.After;

public class HistoryDynamicTilesBackendTest extends StateHistoryBackendTestBase {

    private @Nullable File fHistoryTileFile;
    private @Nullable IStateHistoryBackend fBackend;

    /**
     * Test cleanup
     * @throws IOException
     */
    @After
    public void teardown() throws IOException {
        if (fBackend != null) {
            fBackend.dispose();
        }
        if (fHistoryTileFile != null) {
            fHistoryTileFile.delete();
        }
    }

    @Override
    protected IStateHistoryBackend getBackendForBuilding(long startTime, long endTime) throws IOException {
        fHistoryTileFile = NonNullUtils.checkNotNull(File.createTempFile("2DtestTile", "ht"));
        fBackend = HistoryTileBackendDynamicFactory.createHistoryTreeBackendNewFile("test", fHistoryTileFile, 0, startTime, 1000, true);
        return fBackend;
    }

}
