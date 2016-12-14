package kiwi;

import kiwi.ThreadData.ScanData;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * Created by dbasin on 11/30/15.
 */
public class ScanIndex<K extends Comparable<? super K>> {
    private int[] scanVersions;
    private int[] fromKeys;
    private int[] toKeys;

    //private ScanData[] scans;
    boolean isFirst;
    boolean isOutOfRange;

    private int index;
    private int numOfVersions;
    private Integer min;
    private Integer max;
    private int currKey;

    public ScanIndex(ArrayList<ScanData> scans, int currVersion, K minKey, K maxKey)
    {
        //this.scans = scans;
        this.scanVersions = new int[scans.size()];
        this.fromKeys = new int[scans.size()];
        this.toKeys = new int[scans.size()];

        this.numOfVersions = 0;

        this.min = (minKey != null) ? (Integer)minKey : Integer.MIN_VALUE;
        this.max = (maxKey != null) ? (Integer)maxKey : Integer.MAX_VALUE;

        //noinspection Since15
        scans.sort(new Comparator<ScanData>() {
            @Override
            public int compare(ScanData o1, ScanData o2) {
                return o2.version.get() - o1.version.get();
            }
        });

        //for(int i = 0; i < scans.length; ++i)
        for(ScanData sd: scans)
        {
            if(sd == null) continue;
            if(sd.max.compareTo(min) < 0) continue;
            if(sd.min.compareTo(max) > 0) continue;

            scanVersions[numOfVersions] = sd.version.get();
            fromKeys[numOfVersions] = (int) sd.min;
            toKeys[numOfVersions] = (int) sd.max;
            numOfVersions++;
        }



/*
        Arrays.sort(scanVersions);
        int size = Math.min(numOfVersions,scanVersions.length/2);

        // mirror the array
        for( int i = 0; i < size ; ++i )
        {
            int temp = scanVersions[i];
            scanVersions[i] = scanVersions[scanVersions.length - i - 1];
            scanVersions[scanVersions.length - i - 1] = temp;
        }
*/
        reset(-1);
    }

    public final void reset(int key)
    {
        index = -1;
        isFirst = true;
        currKey = key;
    }

    /***
     *
     *
     * @param version -- we assume that version > 0
     * @return
     */
    public final boolean shouldKeep(int version) {

        //always save the first provided version.
        if(isFirst) return true;
        if(index >= numOfVersions) return false;

        if(fromKeys[index] > currKey) return false;
        if(toKeys[index] < currKey) return false;

        return scanVersions[index] >= version;

    }

    public final void savedVersion(int version)
    {
        isFirst = false;
        index++;
    }
}
