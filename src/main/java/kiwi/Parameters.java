package kiwi;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import util.CombinedGenerator;
import util.IntegerGenerator;
import util.Pair;
import util.ZipfianGenerator;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Parameters of the Java version of the 
 * Synchrobench benchmark.
 *
 * @author Vincent Gramoli
 */
public class Parameters {
    public enum KeyDistribution {

		Uniform	{
					@Override
					public IntegerGenerator createGenerator(int min, int max)
					{
						return new UniformRandom(min,max);
					}
				},
		ShiftedUniform {
					@Override
					public IntegerGenerator createGenerator(int min, int max) { return new ShiftedRandom(min, max);	}
				},
		Zipfian {
			@Override
			public IntegerGenerator createGenerator(int min, int max) {
				return new ZipfianRandomMiddle(min,max);
			}
		},
		ScrambledZipfian {
			@Override
			public IntegerGenerator createGenerator(int min, int max) {
				throw new NotImplementedException();
			}
		},
		SkewedLatest {
			@Override
			public IntegerGenerator createGenerator(int min, int max) {
				throw new NotImplementedException();
			}
		},
		Churn10Perc
				{
					@Override
					public IntegerGenerator createGenerator(int min, int max) {
						return new ChurnPercRandom(min, max, 10);
					}
				},
		Churn5Perc
				{
					@Override
					public IntegerGenerator createGenerator(int min, int max) {
						return new ChurnPercRandom(min, max, 5);
					}
				},
		Churn1Perc
		{
			@Override
			public IntegerGenerator createGenerator(int min, int max) {
				return new ChurnPercRandom(min, max, 1);
			}
		}
		;

		public abstract  IntegerGenerator createGenerator(int min, int max);

		static class ChurnPercRandom extends IntegerGenerator{

			private Random rand;
			int min;
			int max;
			int middle;
			int churnSize;

			public ChurnPercRandom(int min, int max, double churnPerc)
			{
				this.min = min;
				this.max = max;
				this.rand = new Random(System.nanoTime() ^ Thread.currentThread().getId());
				this.middle = (max + min)/2;
				this.churnSize = (int)((max-min)* churnPerc/100);
			}

			@Override
			public int nextInt() {

				if(rand.nextInt(1000) > 100)
				{
					return Math.min(middle + rand.nextInt(churnSize),max);
				}
				else
				{
					return min + rand.nextInt(max - min);
				}
			}

			@Override
			public Pair<Integer, Integer> nextInterval() {
				throw new NotImplementedException();
			}

			@Override
			public double mean() {
				return 0;
			}
		}

		static class UniformRandom extends IntegerGenerator {

			private int min;
			private int max;
			private Random rand;

			public UniformRandom(int min, int max)
			{
				this.min = min;
				this.max = max;
				rand = new Random(System.nanoTime() ^ Thread.currentThread().getId());
			}

			@Override
			public int nextInt() {
				return min + rand.nextInt(max - min);
			}

			@Override
			public Pair<Integer, Integer> nextInterval() {
				int rangeSize = rand.nextInt(1+maxRangeSize-minRangeSize)+minRangeSize;
				int min = nextInt() - rangeSize/2;// rand.nextInt(Parameters.range-rangeSize);

				int minEdge = Parameters.range - rangeSize -1;

				min = min < minEdge? min : minEdge;

				int max = min + rangeSize;

				return new Pair<>(min, max);
			}

			@Override
			public double mean() {
				return (double)(max+min)/2;
			}
		}

		static class ShiftedRandom extends IntegerGenerator{

			private final int shift = CombinedGenerator.getShift();
			private UniformRandom uniformRandom;

			public ShiftedRandom(int min, int max)
			{
				uniformRandom = new UniformRandom(min,max);
			}

			private int shiftValue(int value)
			{
				return value << shift;
			}

			@Override
			public int nextInt() {
				return shiftValue(uniformRandom.nextInt());
			}

			@Override
			public Pair<Integer, Integer> nextInterval() {
				Pair<Integer,Integer> pair = uniformRandom.nextInterval();
				int left = shiftValue(pair.getLeft());
				int right = shiftValue(pair.getRight());

				return new Pair<>(left,right);
			}

			@Override
			public double mean() {
				return ((double)(shiftValue(uniformRandom.min) + shiftValue(uniformRandom.max)))/2;
			}
		}

		static class ZipfianRandomMiddle extends IntegerGenerator{

			private ZipfianGenerator zGen;
			private int min;
			private int max;
			private int middle;
			private Random sideRand;

			public ZipfianRandomMiddle(int min, int max)
			{
				this.min = min;
				this.max = max;
				this.middle = (min + max)/2;
				this.sideRand = new Random(System.nanoTime()^Thread.currentThread().getId());
				zGen = new ZipfianGenerator(middle, max);

			}
			@Override
			public int nextInt() {
				int diff = zGen.nextInt() -middle;
				if(sideRand.nextBoolean())
				{
					return middle -diff;
				}
				else
				{
					return middle + diff;
				}
			}

			@Override
			public Pair<Integer, Integer> nextInterval() {
				throw new NotImplementedException();
			}

			@Override
			public double mean() {
				throw new NotImplementedException();
			}
		}
	}
	public enum FillType { Random, Ordered, DropHalf, Drop90 }

	public static KeyDistribution distribution = KeyDistribution.Uniform;
    public static int numThreads = 1;
    public static int numMilliseconds = 5000;
    public static int numWrites = 40;
	public static int numWriteAlls = 0;
    public static int numSnapshots = 0;
	public static int range = 2048;
	public static int size = 1024;
	public static int warmUp = 5;
	public static int iterations = 1;
    
    public static boolean detailedStats = false;

    public static String benchClassName = new String("skiplists.lockfree.NonBlockingFriendlySkipListMap");
	public static boolean rangeQueries = false;
	public static int minRangeSize = 10;
	public static int maxRangeSize = 20;
	
	public static boolean countKeysInRange = true;
	public static double rebalanceProbPerc = 2;
	public static double sortedRebalanceRatio = 1.8;

	public static FillType fillType = FillType.Random;

	public static AtomicInteger compactionsNum = new AtomicInteger(0);
	public static AtomicInteger engagedChunks = new AtomicInteger(0);
	public static boolean countCompactions = false;

}
