package dao;

import java.util.List;
import java.util.Map;

public interface DBAnnoDao<T> {
	
	public boolean save(T t);
	
	public boolean saveMore(List<T> oblist);
	
	public void delete(Object id,Class<T> clazz) throws Exception;
	
	public void update(T t) throws Exception;
	
	public T get(Object id,Class<T> clazz) throws Exception;
	
	public List<T> findAllByConditions(Map<String,Object> sqlWhereMap,Class<T> clazz,boolean isr) ;
	
	public List<T> findAllOrder(Map<String,Object> sqlWhereMap,Class<T> clazz,int page,int cut,boolean isr);
	
	public int getNum(Map<String,Object> sqlWhereMap,Class<T> clazz,boolean isr) throws Exception;
	
	public boolean deleteList(Map<String, Object> sqlWhereMap, Class<T> clazz,boolean isr);
	
	public List<T> leftJoinList(Map<String,Object> sqlWhereMap,Class<T> left,Class<T> right,int page,int cut,boolean isr);
	
}
