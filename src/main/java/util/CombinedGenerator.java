package util;

import java.util.*;

/**
 * Created by dbasin on 7/7/16.
 */

public  class CombinedGenerator {
    private ArrayList<Integer> prefixes;
    private Map<Integer, UniqueRandomIntGenerator> suffixMap;
    private int idx;
    private Random random;
    private static final int shift = 2;

    public CombinedGenerator(List<Integer> prefixValues) {
        assert prefixValues != null;

        prefixes = new ArrayList<>(prefixValues.size());
        suffixMap = new HashMap<>(prefixValues.size());
        random = new Random();

        for (Integer val : prefixValues) {
            prefixes.add(val);
            suffixMap.put(val, new UniqueRandomIntGenerator(0, 2));
        }

        idx = prefixes.size() - 1;
    }

    public boolean hasNext() {
        return idx >= 0;
    }

    public int next() {
        int randIdx = random.nextInt(idx + 1);
        Integer prefix = prefixes.get(randIdx);

        UniqueRandomIntGenerator gen = suffixMap.get(prefix);
        int suffix = gen.next();

        if (!gen.hasNext()) {
            prefixes.set(randIdx, prefixes.get(idx));
            prefixes.set(idx, prefix);
            idx--;
        }

        return combineNumber(prefix, suffix);
    }

    public static int getShift() {
        return shift;
    }

    public void reset()
    {
        for(UniqueRandomIntGenerator gen : suffixMap.values())
        {
            gen.reset();
        }

        idx = prefixes.size() -1;
    }

    private int combineNumber(int prefix, int suffix)
    {
        int num = prefix << shift;
        num = num | suffix;

        return num;
    }
}

