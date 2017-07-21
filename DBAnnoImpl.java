package impl;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import annotation.Column;
import annotation.Entity;
import dao.DBAnnoDao;
import util.DBCPUtil;

public class DBAnnoImpl<T> implements DBAnnoDao<T> {

	// 表的别名
	private static final String TABLE_ALIAS = "t";
	private static final String TABLE_JOIN_1 = "u";
	private Connection conn;

	public boolean save(T t) {
		Class<?> clazz = t.getClass();
		// 获得表名
		String tableName = null;
		PreparedStatement ps = null;
		try {
			tableName = getTableName(clazz);
		} catch (Exception e1) {
			e1.printStackTrace();
			return false;
		}
		// 获得字段
		StringBuilder fieldNames = new StringBuilder(); // 字段名
		List<Object> fieldValues = new ArrayList<Object>(); // 字段值
		StringBuilder placeholders = new StringBuilder(); // 占位符
		Field[] fields = clazz.getDeclaredFields();
		try {
			for (Field field : fields) {
				PropertyDescriptor pd = new PropertyDescriptor(field.getName(), t.getClass());
				if (pd.getReadMethod().invoke(t) != null) {
					if (field.isAnnotationPresent(Column.class) && field.getAnnotation(Column.class).need()) {
						fieldNames.append(field.getAnnotation(Column.class).value()).append(",");
						fieldValues.add(pd.getReadMethod().invoke(t));
						placeholders.append("?").append(",");
					}

				}

			}
			// 删除最后一个逗号
			fieldNames.deleteCharAt(fieldNames.length() - 1);
			placeholders.deleteCharAt(placeholders.length() - 1);

			// 拼接sql
			StringBuilder sql = new StringBuilder("");
			sql.append("replace into ").append(tableName).append(" (").append(fieldNames.toString())
					.append(") values (").append(placeholders).append(")");
			conn = DBCPUtil.getConnection();
			ps = conn.prepareStatement(sql.toString());
			// 设置SQL参数占位符的值
			setParameter(fieldValues, ps, false);
			// 执行SQL
			ps.executeUpdate();
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				ps.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return true;
	}

	public boolean saveMore(List<T> oblist) {
		if (oblist.isEmpty() || oblist == null)
			return true;
		else {

			Class<?> clazz = oblist.get(0).getClass();
			String tableName = null;
			try {
				tableName = getTableName(clazz);
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
			StringBuilder fieldNames = new StringBuilder();
			StringBuilder placeholders = new StringBuilder();
			List<Object> fieldValues = new ArrayList<Object>();
			Field[] fields = clazz.getDeclaredFields();
			PropertyDescriptor pd = null;
			PreparedStatement ps = null;
			try {
				for (Field field : fields) {
					pd = new PropertyDescriptor(field.getName(), clazz);
					if (pd.getReadMethod().invoke(oblist.get(0)) != null) {
						if (field.isAnnotationPresent(Column.class) && field.getAnnotation(Column.class).need()) {
							fieldNames.append(field.getAnnotation(Column.class).value()).append(",");
							placeholders.append("?").append(",");
						}
					}
				}
				fieldNames.deleteCharAt(fieldNames.length() - 1);
				placeholders.deleteCharAt(placeholders.length() - 1);

				StringBuilder sql = new StringBuilder("");
				sql.append("replace into ").append(tableName).append(" (").append(fieldNames.toString())
						.append(") values (").append(placeholders).append(")");
				conn = DBCPUtil.getConnection();
				ps = conn.prepareStatement(sql.toString());
				for (int i = 0; i < oblist.size(); i++) {
					fieldValues.clear();
					for (Field field : fields) {
						pd = new PropertyDescriptor(field.getName(), clazz);
						if (pd.getReadMethod().invoke(oblist.get(i)) != null) {
							if (field.isAnnotationPresent(Column.class) && field.getAnnotation(Column.class).need()) {
								fieldValues.add(pd.getReadMethod().invoke(oblist.get(i)));
							}
						}
					}
					setParameter(fieldValues, ps, false);
					ps.addBatch();
					if ((i + 1) % 1000 == 0)
						ps.executeBatch();
				}
				ps.executeBatch();
				conn.commit();
				return true;
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			} finally {
				try {
					if (ps != null)
						ps.close();
					if (conn != null)
						conn.close();
				} catch (Exception e2) {
					e2.printStackTrace();
				}
			}
		}
	}

	public void delete(Object id, Class<T> clazz) throws Exception {
		String tableName = getTableName(clazz);
		String idFieldName = "";
		boolean flag = false;
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			if (field.isAnnotationPresent(Column.class) && field.getAnnotation(Column.class).pk()) {
				idFieldName = field.getAnnotation(Column.class).value();
				flag = true;
				break;
			}
		}
		if (!flag) {
			throw new Exception(clazz.getName() + " object not found id property.");
		}

		String sql = "delete from " + tableName + " where " + idFieldName + "=?";
		conn = DBCPUtil.getConnection();
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setObject(1, id);
		ps.execute();
		conn.commit();
		ps.close();
		conn.close();

		// System.out.println(sql + "\n" + clazz.getSimpleName() + "删除成功!");
	}

	public void update(T t) throws Exception {
		Class<?> clazz = t.getClass();
		String tableName = getTableName(clazz);
		List<Object> fieldNames = new ArrayList<Object>();
		List<Object> fieldValues = new ArrayList<Object>();
		List<String> placeholders = new ArrayList<String>();
		String idFieldName = "";
		Object idFieldValue = "";
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			PropertyDescriptor pd = new PropertyDescriptor(field.getName(), t.getClass());
			if (pd.getReadMethod().invoke(t) != null) {
				if (field.isAnnotationPresent(Column.class) && !field.getAnnotation(Column.class).pk()) {
					fieldNames.add(field.getAnnotation(Column.class).value());
					fieldValues.add(pd.getReadMethod().invoke(t));
					placeholders.add("?");
				} else if (field.getAnnotation(Column.class).pk()) {
					idFieldName = field.getAnnotation(Column.class).value();
					idFieldValue = pd.getReadMethod().invoke(t);
				}
			}
		}
		fieldNames.add(idFieldName);
		fieldValues.add(idFieldValue);
		placeholders.add("?");
		StringBuilder sql = new StringBuilder("");
		sql.append("update ").append(tableName).append(" set ");
		int index = fieldNames.size() - 1;
		for (int i = 0; i < index; i++) {
			sql.append(fieldNames.get(i)).append("=").append(placeholders.get(i)).append(",");
		}
		sql.deleteCharAt(sql.length() - 1).append(" where ").append(fieldNames.get(index)).append("=").append("?");

		conn = DBCPUtil.getConnection();
		PreparedStatement ps = conn.prepareStatement(sql.toString());
		setParameter(fieldValues, ps, false);
		ps.execute();
		conn.commit();
		ps.close();
		conn.close();

	}

	public T get(Object id, Class<T> clazz) {
		String idFieldName = "";
		Field[] fields = clazz.getDeclaredFields();
		boolean flag = false;
		for (Field field : fields) {
			if (field.isAnnotationPresent(Column.class) && field.getAnnotation(Column.class).pk()) {
				idFieldName = field.getAnnotation(Column.class).value();
				flag = true;
				break;
			}
		}

		if (!flag) {
			try {
				throw new Exception(clazz.getName() + " object not found id property.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		Map<String, Object> sqlWhereMap = new HashMap<String, Object>();
		sqlWhereMap.put(TABLE_ALIAS + "." + idFieldName, id);

		List<T> list = null;
		list = findAllByConditions(sqlWhereMap, clazz, false);

		return list.size() > 0 ? list.get(0) : null;
	}

	@SuppressWarnings("unchecked")
	public List<T> findAllByConditions(Map<String, Object> sqlWhereMap, Class<T> clazz, boolean isr) {
		List<T> list = new ArrayList<T>();
		String tableName = null;
		try {
			tableName = getTableName(clazz);
		} catch (Exception e) {
			e.printStackTrace();
		}
		StringBuffer fieldNames = new StringBuffer();
		Field[] fields = clazz.getDeclaredFields();
		List<Field> fl = new ArrayList<>();
		for (Field field : fields) {
			String propertyName = field.getName();
			if (field.isAnnotationPresent(Column.class)) {
				fieldNames.append(TABLE_ALIAS + "." + field.getAnnotation(Column.class).value()).append(" as ")
						.append(propertyName).append(",");
				fl.add(field);
			}
		}
		fieldNames.deleteCharAt(fieldNames.length() - 1);

		String sql = "select " + fieldNames + " from " + tableName + " " + TABLE_ALIAS;
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Object> values = null;
		if (sqlWhereMap != null) {
			List<Object> sqlWhereWithValues = getSqlWhereWithValues(sqlWhereMap);
			if (sqlWhereWithValues != null) {
				String sqlWhere = (String) sqlWhereWithValues.get(0);
				sql += sqlWhere;
				values = (List<Object>) sqlWhereWithValues.get(1);
			}
		}

		// System.out.println(sql);
		try {
			if (values != null) {
				conn = DBCPUtil.getConnection();
				ps = conn.prepareStatement(sql);
				setParameter(values, ps, isr);
			} else {
				conn = DBCPUtil.getConnection();
				ps = conn.prepareStatement(sql);
			}
			Field[] fs = fl.toArray(fields);
			rs = ps.executeQuery();
			conn.commit();
			while (rs.next()) {
				T t = clazz.newInstance();
				initObject(t, fs, rs);
				list.add(t);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
				ps.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		return list;
	}

	@SuppressWarnings("unchecked")
	public int getNum(Map<String, Object> sqlWhereMap, Class<T> clazz, boolean isr) {
		int allnum = 0;
		String tableName = null;
		try {
			tableName = getTableName(clazz);
		} catch (Exception e) {
			e.printStackTrace();
		}
		String sql = "select count(*) from " + tableName + " " + TABLE_ALIAS;
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Object> values = null;
		if (sqlWhereMap != null) {
			List<Object> sqlWhereWithValues = getSqlWhereWithValues(sqlWhereMap);
			if (sqlWhereWithValues != null) {
				String sqlWhere = (String) sqlWhereWithValues.get(0);
				sql += sqlWhere;
				values = (List<Object>) sqlWhereWithValues.get(1);
			}
		}
		try {
			if (values != null) {
				conn = DBCPUtil.getConnection();
				ps = conn.prepareStatement(sql);
				setParameter(values, ps, isr);
			} else {
				conn = DBCPUtil.getConnection();
				ps = conn.prepareStatement(sql);
			}
			rs = ps.executeQuery();
			conn.commit();
			rs.next();
			allnum = rs.getInt(1);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
				ps.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

		return allnum;
	}

	@SuppressWarnings("unchecked")
	public List<T> findAllOrder(Map<String, Object> sqlWhereMap, Class<T> clazz, int page, int cut, boolean isr) {
		List<T> list = new ArrayList<T>();
		String tableName;
		PreparedStatement ps = null;
		List<Object> values = null;
		ResultSet rs = null;
		try {
			tableName = getTableName(clazz);
			StringBuffer fieldNames = new StringBuffer();
			String order = "";
			String limit = " limit " + (page - 1) * cut + "," + cut;
			Field[] fields = clazz.getDeclaredFields();
			List<Field> fl = new ArrayList<>();
			for (Field field : fields) {
				String propertyName = field.getName();
				if (field.isAnnotationPresent(Column.class)) {
					fieldNames.append(TABLE_ALIAS + "." + field.getAnnotation(Column.class).value()).append(" as ")
							.append(propertyName).append(",");
					if (field.getAnnotation(Column.class).order())
						order = " order by " + field.getAnnotation(Column.class).value() + " DESC";
					fl.add(field);
				}
			}
			fieldNames.deleteCharAt(fieldNames.length() - 1);

			String sql = "select " + fieldNames + " from " + tableName + " " + TABLE_ALIAS;
			if (sqlWhereMap != null) {
				List<Object> sqlWhereWithValues = getSqlWhereWithValues(sqlWhereMap);
				if (sqlWhereWithValues != null) {
					String sqlWhere = (String) sqlWhereWithValues.get(0);
					sql += sqlWhere;
					values = (List<Object>) sqlWhereWithValues.get(1);
				}
			}
			sql = sql + order + limit;
			fields = fl.toArray(fields);
			if (values != null) {
				conn = DBCPUtil.getConnection();
				ps = conn.prepareStatement(sql);
				setParameter(values, ps, isr);
			} else {
				conn = DBCPUtil.getConnection();
				ps = conn.prepareStatement(sql);
			}

			rs = ps.executeQuery();
			conn.commit();
			while (rs.next()) {
				T t = clazz.newInstance();
				initObject(t, fields, rs);
				list.add(t);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				rs.close();
				ps.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	/**
	 * 根据结果集初始化对象
	 */
	private void initObject(T t, Field[] fields, ResultSet rs)
			throws SQLException, IntrospectionException, IllegalAccessException, InvocationTargetException {
		for (Field field : fields) {
			if (field == null)
				continue;
			String propertyName = field.getName();
			Object paramVal = null;
			Class<?> clazzField = field.getType();
			if (clazzField == String.class) {
				paramVal = rs.getString(propertyName);
			} else if (clazzField == short.class || clazzField == Short.class) {
				paramVal = rs.getShort(propertyName);
			} else if (clazzField == int.class || clazzField == Integer.class) {
				paramVal = rs.getInt(propertyName);
			} else if (clazzField == long.class || clazzField == Long.class) {
				paramVal = rs.getLong(propertyName);
			} else if (clazzField == float.class || clazzField == Float.class) {
				paramVal = rs.getFloat(propertyName);
			} else if (clazzField == double.class || clazzField == Double.class) {
				paramVal = rs.getDouble(propertyName);
			} else if (clazzField == boolean.class || clazzField == Boolean.class) {
				paramVal = rs.getBoolean(propertyName);
			} else if (clazzField == byte.class || clazzField == Byte.class) {
				paramVal = rs.getByte(propertyName);
			} else if (clazzField == char.class || clazzField == Character.class) {
				paramVal = rs.getCharacterStream(propertyName);
			} else if (clazzField == Date.class || clazzField == Timestamp.class) {
				paramVal = rs.getTimestamp(propertyName);
			} else if (clazzField.isArray()) {
				paramVal = rs.getString(propertyName).split(","); // 以逗号分隔的字符串
			}
			PropertyDescriptor pd = new PropertyDescriptor(propertyName, t.getClass());
			pd.getWriteMethod().invoke(t, paramVal);
		}
	}

	/**
	 * 根据条件，返回sql条件和条件中占位符的值
	 * 
	 * @param sqlWhereMap
	 *            key：字段名 value：字段值
	 * @return 第一个元素为SQL条件，第二个元素为SQL条件中占位符的值
	 */
	@SuppressWarnings("unchecked")
	private List<Object> getSqlWhereWithValues(Map<String, Object> sqlWhereMap) {
		if (sqlWhereMap.size() < 1)
			return null;
		List<Object> list = new ArrayList<Object>();
		List<Object> fieldValues = new ArrayList<Object>();
		StringBuffer sqlWhere = new StringBuffer(" where ");
		Set<Entry<String, Object>> entrySets = sqlWhereMap.entrySet();
		for (Iterator<Entry<String, Object>> iteraotr = entrySets.iterator(); iteraotr.hasNext();) {
			Entry<String, Object> entrySet = iteraotr.next();
			Object value = entrySet.getValue();
			if (value.getClass() == ArrayList.class) {
				StringBuffer sqlmap = new StringBuffer(" IN (");
				for (Object o : (List<Object>) value) {
					fieldValues.add(o);
					sqlmap.append("?,");
				}
				sqlmap.delete(sqlmap.lastIndexOf(","), sqlmap.length()).append(")");
				sqlWhere.append(entrySet.getKey()).append(sqlmap).append(" and ");
			} else {
				if (value.getClass() == String.class) {
					sqlWhere.append(entrySet.getKey()).append(" like ").append("?").append(" and ");
				} else {
					sqlWhere.append(entrySet.getKey()).append("=").append("?").append(" and ");
				}
				fieldValues.add(value);
			}
		}
		if (sqlWhere.lastIndexOf("and") != -1) {
			sqlWhere.delete(sqlWhere.lastIndexOf("and"), sqlWhere.length());
		}
		list.add(sqlWhere.toString());
		list.add(fieldValues);
		return list;
	}

	/**
	 * 获得表名
	 */
	private String getTableName(Class<?> clazz) throws Exception {
		if (clazz.isAnnotationPresent(Entity.class)) {
			Entity entity = clazz.getAnnotation(Entity.class);
			return entity.value();
		} else {
			throw new Exception(clazz.getName() + " is not Entity Annotation.");
		}
	}

	/**
	 * 设置SQL参数占位符的值
	 */
	private void setParameter(List<Object> values, PreparedStatement ps, boolean isSearch) throws SQLException {
		for (int i = 1; i <= values.size(); i++) {
			Object fieldValue = values.get(i - 1);
			Class<?> clazzValue = fieldValue.getClass();
			if (clazzValue == String.class) {
				if (isSearch)
					ps.setString(i, "%" + (String) fieldValue + "%");
				else
					ps.setString(i, (String) fieldValue);

			} else if (clazzValue == boolean.class || clazzValue == Boolean.class) {
				ps.setBoolean(i, (Boolean) fieldValue);
			} else if (clazzValue == byte.class || clazzValue == Byte.class) {
				ps.setByte(i, (Byte) fieldValue);
			} else if (clazzValue == char.class || clazzValue == Character.class) {
				ps.setObject(i, fieldValue, Types.CHAR);
			} else if (clazzValue == Date.class || clazzValue == Timestamp.class) {
				ps.setTimestamp(i, new Timestamp(((Date) fieldValue).getTime()));
			} else if (clazzValue.isArray()) {
				Object[] arrayValue = (Object[]) fieldValue;
				StringBuffer sb = new StringBuffer();
				for (int j = 0; j < arrayValue.length; j++) {
					sb.append(arrayValue[j]).append("、");
				}
				ps.setString(i, sb.deleteCharAt(sb.length() - 1).toString());
			} else {
				ps.setObject(i, fieldValue, Types.NUMERIC);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean deleteList(Map<String, Object> sqlWhereMap, Class<T> clazz, boolean isr) {
		String tableName = null;
		try {
			tableName = getTableName(clazz);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		List<Object> values = null;
		String sql = "delete from " + tableName;
		PreparedStatement ps = null;
		try {
			if (sqlWhereMap != null) {
				List<Object> sqlWhereWithValues = getSqlWhereWithValues(sqlWhereMap);
				if (sqlWhereWithValues != null) {
					String sqlWhere = (String) sqlWhereWithValues.get(0);
					sql += sqlWhere;
					values = (List<Object>) sqlWhereWithValues.get(1);
				}
			}
			conn = DBCPUtil.getConnection();
			ps = conn.prepareStatement(sql);
//			System.out.println(sql);
			if (values != null) {
				conn = DBCPUtil.getConnection();
				ps = conn.prepareStatement(sql);
				setParameter(values, ps, isr);
			} else {
				conn = DBCPUtil.getConnection();
				ps = conn.prepareStatement(sql);
			}
			ps.execute();
			conn.commit();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if (ps != null)
					ps.close();
				if (conn != null)
					conn.close();
			} catch (Exception e2) {
				e2.printStackTrace();
				return false;
			}
		}
	}

	@SuppressWarnings("unchecked")
	public List<T> leftJoinList(Map<String, Object> sqlWhereMap, Class<T> left, Class<T> right, int page, int cut,
			boolean isr) {
		List<T> list = new ArrayList<T>();
		String left_name = null;
		String right_name = null;
		try {
			left_name = getTableName(left);
			right_name = getTableName(right);
		} catch (Exception e) {
			e.printStackTrace();
		}
		StringBuffer fieldNames = new StringBuffer();
		StringBuffer joinNames = new StringBuffer();
		Field[] lefts = left.getDeclaredFields();
		Field[] rights = right.getDeclaredFields();
		for (Field field : lefts) {
			String propertyName = field.getName();
			if (field.isAnnotationPresent(Column.class)) {
				fieldNames.append(TABLE_ALIAS + "." + field.getAnnotation(Column.class).value()).append(" as ")
						.append(propertyName).append(",");
				if (field.getAnnotation(Column.class).foreign()) {
					joinNames.append(TABLE_ALIAS + "." + field.getAnnotation(Column.class).value() + "=");
				}
			}
		}
		for (Field field : rights) {
			if (field.isAnnotationPresent(Column.class)) {
				if (field.getAnnotation(Column.class).foreign()) {
					joinNames.append(TABLE_JOIN_1 + "." + field.getAnnotation(Column.class).value());
				}
				if (field.getAnnotation(Column.class).join()) {
					fieldNames.append(TABLE_JOIN_1 + "." + field.getAnnotation(Column.class).value()).append(" as ")
							.append(field.getName()).append(",");
				}
			}
		}
		fieldNames.deleteCharAt(fieldNames.length() - 1);
		String sql = "select " + fieldNames + " from " + left_name + " " + TABLE_ALIAS + " left join " + right_name
				+ " " + TABLE_JOIN_1 + " on " + joinNames;
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<Object> values = null;
		if (sqlWhereMap != null) {
			List<Object> sqlWhereWithValues = getSqlWhereWithValues(sqlWhereMap);
			if (sqlWhereWithValues != null) {
				String sqlWhere = (String) sqlWhereWithValues.get(0);
				sql += sqlWhere;
				values = (List<Object>) sqlWhereWithValues.get(1);
			}
		}
		try {
			if (values != null) {
				conn = DBCPUtil.getConnection();
				ps = conn.prepareStatement(sql);
				setParameter(values, ps, isr);
			} else {
				conn = DBCPUtil.getConnection();
				ps = conn.prepareStatement(sql);
			}
			rs = ps.executeQuery();
			conn.commit();
			while (rs.next()) {
				T t = left.newInstance();
				initObject(t, lefts, rs);
				list.add(t);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
				ps.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		return list;
	}
}
