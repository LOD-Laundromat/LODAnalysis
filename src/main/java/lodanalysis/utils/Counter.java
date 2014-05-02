package lodanalysis.utils;

public class Counter {
	private int count = 0;

	public Counter() {

	}
	public Counter(int initVal) {
		count = initVal;
	}

	public void increase() {
		count++;
	}
	public int get() {
		return count;
	}

	public String toString() {
		return Integer.toString(count);
	}
}
