package util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Created by dbasin on 7/7/16.
 */
public class UniqueRandomIntGenerator{
    private ArrayList<Integer> values;
    private int idx = 0;
    private Random rand = new Random();

    public UniqueRandomIntGenerator(int from, int to)
    {
        values = new ArrayList<>(to - from + 1);
        for(int i = from; i < to; ++i)
        {
            values.add(i);
        }

        idx = values.size()-1;
    }

    public UniqueRandomIntGenerator(Set<Integer> sourceValues)
    {
        assert sourceValues != null;

        values = new ArrayList<>(sourceValues.size());

        for(Integer val : sourceValues)
        {
            values.add(val);
        }

        idx = values.size() -1;
    }

    public boolean hasNext()
    {
        return idx >=0;
    }

    public int next()
    {
        if(idx < 0 ) return -1;
        int randIdx = rand.nextInt(idx + 1);

        Integer nextVal = values.get(randIdx);

        //swap values here
        values.set(randIdx,values.get(idx));
        values.set(idx,nextVal);


        idx--;

        return nextVal;
    }

    public void reset()
    {
        idx = values.size() - 1;
    }

    public int last()
    {
        int last = idx + 1;
        if(last >= values.size()) return  -1;
        return values.get(last);
    }

    public List<Integer> getValues()
    {
        return values;
    }

}
