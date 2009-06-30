package org.apache.activemq.apollo;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Combinator objects are used to compute all the possible combinations given a set of combination options.
 * This class is generally use in conjunction with TestNG test cases generate the @Factory and @DataProvider 
 * results.
 * 
 * @author chirino
 */
public class Combinator {

	private LinkedHashMap<String, ComboOption> comboOptions = new LinkedHashMap<String, ComboOption>();
	private int annonymousAttributeCounter;

	static class ComboOption {
		final String attribute;
		final LinkedHashSet<Object> values = new LinkedHashSet<Object>();

		public ComboOption(String attribute, Collection<Object> options) {
			this.attribute = attribute;
			this.values.addAll(options);
		}
	}

	public void put(String attribute, Object... options) {
		ComboOption co = this.comboOptions.get(attribute);
		if (co == null) {
			this.comboOptions.put(attribute, new ComboOption(attribute, Arrays.asList(options)));
		} else {
			co.values.addAll(Arrays.asList(options));
		}
	}
	
	public void add(Object... options) {
		put(""+(annonymousAttributeCounter++), options);
	}
	
//	@SuppressWarnings("unchecked")
//	public void addFromContext(ApplicationContext applicationContext, String name) {
//		List<Object> list = (List)applicationContext.getBean(name);
//		Object[] options = list.toArray();
//		add(options);
//	}
//	
//	public void addAllFromContext(ApplicationContext applicationContext, String name) {
//		List<List<Object>> list = (List)applicationContext.getBean(name);
//		for (List<Object> l : list) {
//			Object[] options = l.toArray();
//			add(options);
//		}
//	}


	public List<Map<String, Object>> combinations() {
		List<Map<String, Object>> expandedOptions = new ArrayList<Map<String, Object>>();
		expandCombinations(new ArrayList<ComboOption>(comboOptions.values()), expandedOptions);
		return expandedOptions;

	}

	private void expandCombinations(List<ComboOption> optionsLeft, List<Map<String, Object>> expandedCombos) {
		if (!optionsLeft.isEmpty()) {
			Map<String, Object> map;
			if (comboOptions.size() == optionsLeft.size()) {
				map = new LinkedHashMap<String, Object>();
				expandedCombos.add(map);
			} else {
				map = expandedCombos.get(expandedCombos.size() - 1);
			}

			LinkedList<ComboOption> l = new LinkedList<ComboOption>(optionsLeft);
			ComboOption comboOption = l.removeFirst();
			int i = 0;
			for (Iterator<Object> iter = comboOption.values.iterator(); iter.hasNext();) {
				Object value = iter.next();
				if (i != 0) {
					map = new LinkedHashMap<String, Object>(map);
					expandedCombos.add(map);
				}
				map.put(comboOption.attribute, value);
				expandCombinations(l, expandedCombos);
				i++;
			}
		}
	}

	/**
	 * Creates a bean for each combination of the type specified by clazz arguement and uses setter/field 
	 * injection to initialize the Bean with the combination values.
	 * 
	 * @param <T>
	 * @param clazz
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public <T> Object[] combinationsAsBeans(Class<T> clazz) throws Exception {
		List<Map<String, Object>> combinations = combinations();
		List<T> rc = new ArrayList<T>(combinations.size());
		for (Map<String, Object> combination : combinations) {
			T instance = clazz.newInstance();
			
			for (Entry<String, Object> entry : combination.entrySet()) {
				String key = entry.getKey();
				Object value = entry.getValue();
				try {
					// Try setter injection..
					Method method = clazz.getMethod("set"+ucfc(key), value.getClass());
					method.invoke(instance, new Object[]{value});
				} catch (Exception ignore) {
					// Try property injection..
					Field declaredField = clazz.getDeclaredField(key);
					declaredField.set(instance, value);
				}
			}
			
			rc.add(instance);
		}
		Object[] t = new Object[rc.size()];
		rc.toArray(t);
		return t;
	}

	/**
	 * Upper case the first character.
	 * @param key
	 * @return
	 */
	static private String ucfc(String key) {
		return key.substring(0,1).toUpperCase()+key.substring(1);
	}

	public Object[][] combinationsAsParameterArgs() {
		List<Map<String, Object>> combinations = combinations();
		Object[][] rc = new Object[combinations.size()][];
		int i=0;
		for (Map<String, Object> combination : combinations) {
			int j=0;
			Object[] arg = new Object[combination.size()];
			for (Object object : combination.values()) {
				arg[j++] = object;
			}
			rc[i++] = arg;
		}
		return rc;
	}

}
