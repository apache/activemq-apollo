/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.util;

import java.lang.reflect.Array;
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
import java.util.Set;
import java.util.Map.Entry;

/**
 * Combinator objects are used to compute all the possible combinations given a set of combination options.
 * This class is generally use in conjunction with TestNG test cases generate the @Factory and @DataProvider 
 * results.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Combinator {

    private Combinator parent;
    private ArrayList<Combinator> children = new ArrayList<Combinator>();
    
	private LinkedHashMap<String, ComboOption> comboOptions = new LinkedHashMap<String, ComboOption>();
	private int annonymousAttributeCounter;
	
	public Combinator() {
    }
	
    public Combinator(Combinator parent) {
        this.parent = parent;
    }

    // For folks who like to use static imports to achieve a more fluent usage API.
	public static Combinator combinator() {
		return new Combinator();
	}
		
	static class ComboOption {
		final String attribute;
		final LinkedHashSet<Object> values = new LinkedHashSet<Object>();

		public ComboOption(String attribute, Collection<Object> options) {
			this.attribute = attribute;
			this.values.addAll(options);
		}
	}

	
	public ArrayList<Combinator> all() {
        ArrayList<Combinator> rc = new ArrayList<Combinator>();
        root()._all(rc);
        return rc;
    }
    
	private void _all(ArrayList<Combinator> rc) {
	    rc.add(this);
        for (Combinator c : children) {
            c._all(rc);
        }	    
	}
	
	public Combinator put(String attribute, Object... options) {
		ComboOption co = this.comboOptions.get(attribute);
		if (co == null) {
			this.comboOptions.put(attribute, new ComboOption(attribute, Arrays.asList(options)));
		} else {
			co.values.addAll(Arrays.asList(options));
		}
		return this;
	}
	
    public Combinator and() {
        Combinator combinator = new Combinator(this);
        children.add(combinator);
        return combinator;
    }

	
	public Combinator add(Object... options) {
		put(""+(annonymousAttributeCounter++), options);
		return this;
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


	public Set<Map<String, Object>> combinations() {
	    return root()._combinations();
	}

    private Combinator root() {
        Combinator c=this;
        while( c.parent!=null ) {
            c = c.parent;
        }
        return c;
    }

    private Set<Map<String, Object>> _combinations() {
        LinkedHashSet<Map<String, Object>> rc = new LinkedHashSet<Map<String, Object>>();
        List<Map<String, Object>> expandedOptions = new ArrayList<Map<String, Object>>();
        expandCombinations(new ArrayList<ComboOption>(comboOptions.values()), expandedOptions);
        rc.addAll(expandedOptions);
        for (Combinator c : children) {
            rc.addAll(c._combinations());
        }
		return rc;
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
		Set<Map<String, Object>> combinations = combinations();
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
			if( instance instanceof CombinationAware) {
				((CombinationAware)instance).setCombination(combination);
			}
			rc.add(instance);
		}
		Object[] t = new Object[rc.size()];
		rc.toArray(t);
		return t;
	}

	public <T> Object[][] combinationsAsParameterArgBeans(Class<T> clazz) throws Exception {
		Object[] x = combinationsAsBeans(clazz);
		Object[][]rc = new Object[x.length][];
		for (int i = 0; i < rc.length; i++) {
			rc[i] = new Object[] {x[i]};
		}
		return rc;
	}
	
	public interface BeanFactory<T> {
		T createBean() throws Exception;
		Class<T> getBeanClass();
	}
	
	public interface CombinationAware {

		void setCombination(Map<String, Object> combination);
	}

	
	/**
	 * Creates a bean for each combination of the type specified by clazz argument and uses setter/field 
	 * injection to initialize the Bean with the combination values.
	 * 
	 * @param clazz
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public <T> T[] asBeans(BeanFactory<T> factory) throws Exception {
		Set<Map<String, Object>> combinations = combinations();
		List<Object> rc = new ArrayList<Object>(combinations.size());
		
		Class<? extends Object> clazz=null;
		for (Map<String, Object> combination : combinations) {
			Object instance = factory.createBean();
			if( clazz == null ) {
				clazz = instance.getClass();
			}
			for (Entry<String, Object> entry : combination.entrySet()) {
				String key = entry.getKey();
				Object value = entry.getValue();
				try {
					// Try setter injection..
					Method method = clazz.getMethod("set"+ucfc(key), value.getClass());
					method.invoke(instance, new Object[]{value});
				} catch (Exception ignore) {
					// Try property injection..
					setField(clazz, instance, key, value);
				}
			}
			
			if( instance instanceof CombinationAware) {
				((CombinationAware)instance).setCombination(combination);
			}
			rc.add(instance);
		}
		
		T[] t = toArray(factory, rc);
		rc.toArray(t);
		return t;
	}

    @SuppressWarnings("unchecked")
    private <T> T[] toArray(BeanFactory<T> factory, List<Object> rc) {
        return (T[]) Array.newInstance(factory.getBeanClass(), rc.size());
    }

	private void setField(Class<? extends Object> clazz, Object instance, String key, Object value) throws NoSuchFieldException, IllegalAccessException {
		while( clazz!= null ) {
			try {
				Field declaredField = clazz.getDeclaredField(key);
				declaredField.setAccessible(true);
				declaredField.set(instance, value);
				return;
			} catch (NoSuchFieldException e) {
				// Field declaration may be in a parent class... keep looking.
				clazz = clazz.getSuperclass();
			}
		}
	}

	public <T> Object[][] combinationsAsParameterArgBeans(BeanFactory<T> factory) throws Exception {
		Object[] x = asBeans(factory);
		Object[][]rc = new Object[x.length][];
		for (int i = 0; i < rc.length; i++) {
			rc[i] = new Object[] {x[i]};
		}
		return rc;
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
		Set<Map<String, Object>> combinations = combinations();
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
