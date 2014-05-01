package lodanalysis.utils;

public class Counter {
	private Integer count = 0;

	public Counter() {

	}
	public Counter(Integer initVal) {
		count = initVal;
	}

	public void increase() {
		count++;
	}
	public Integer get() {
		return count;
	}

	public String toString() {
		return count.toString();
	}
}
