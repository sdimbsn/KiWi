package kiwi;

import java.io.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by dbasin on 1/24/16.
 */
public class KiWiMapChunkProf extends KiWiMap {

    private final AtomicInteger opsDone = new AtomicInteger(0);
    private final int opsPerSnap = Parameters.range/20;

    private int snapCount = 0;
    private int totalOps = 0;

    private static final String profDirPath =  new String("./../output/data");
    private static final String profFileName =  "chunkStat.csv";

    BufferedWriter writer = null;

    private File outFile;

    private volatile boolean isUnderSnapshot = false;

    private void trySnapshot()
    {

        int currOps = opsDone.getAndIncrement();

        if(currOps >= opsPerSnap)
        {
            synchronized(this)
            {
                currOps = opsDone.get();

                if( currOps <  opsPerSnap ) return;

                isUnderSnapshot = true;

                totalOps += currOps;
                snapCount++;

                printStatsLine(kiwi.calcChunkStatistics());

                opsDone.set(0);

                isUnderSnapshot = false;

            }
        }
    }

    private void printStatsHeader()
    {
        try {
            writer.write("FillType\tOps\tTotalItems\tChunks\tSorted\tChunkItems\tOccupied\tDuplicates\tNulls\tRemoved\tKeyJumps\tValJumps");
            writer.newLine();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void printStatsLine(DebugStats ds)
    {
        StringBuffer sb = new StringBuffer();

        char delimeter = '\t';
        sb.append(Parameters.fillType);
        sb.append(delimeter);

        sb.append(totalOps).append(delimeter).
                append(ds.itemCount).append(delimeter).
            append(ds.chunksCount).append(delimeter).
            append(ds.sortedCells/ds.chunksCount).append(delimeter).
            append(ds.itemCount/ds.chunksCount).append(delimeter).
            append(ds.occupiedCells/ds.chunksCount).append(delimeter).
            append(ds.duplicatesCount/ds.chunksCount).append(delimeter).
            append(ds.nulItemsCount/ds.chunksCount).append(delimeter).
            append(ds.removedItems/ds.chunksCount).append(delimeter).
            append(ds.jumpKeyCount/ds.chunksCount).append(delimeter).
            append(ds.jumpValCount/ds.chunksCount);

        try {
            writer.write(sb.toString());
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public KiWiMapChunkProf()
    {
        super();
        new File(profDirPath).mkdirs();

        outFile = new File(profDirPath + "/" +profFileName);

       // if(outFile.exists())
         //   outFile.delete();

        try {
            outFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }



        try {
            writer = new BufferedWriter(new FileWriter(outFile.getCanonicalPath()));
            printStatsHeader();

        } catch(IOException ioe) {
            ioe.printStackTrace();
        }

    }
    @Override
    public Integer put(Integer key, Integer val)
    {
        // busy wait here
        while(isUnderSnapshot)
            ;

        super.put(key, val);

        trySnapshot();

        return null;

    }

    @Override
    public Integer remove(Object key)
    {
        // busy wait here
        while(isUnderSnapshot)
            ;

        super.remove(key);

        trySnapshot();

        return null;
    }
}
