package lodanalysis;

public abstract class RuneableClass {
	protected Entry entry;
	public RuneableClass(Entry entry) {
		Class<?> enclosingClass = getClass().getEnclosingClass();
		String className = null;
		if (enclosingClass != null) {
			className = enclosingClass.getName();
		} else {
			className = getClass().getName();
		}
		System.out.println("running class: " + className);
		this.entry = entry;
	}
}
