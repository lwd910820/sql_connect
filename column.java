package annotation;  
  
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;  
  
/** 
 * 标识数据库字段的名称 
 * 
 */  
@Retention(RetentionPolicy.RUNTIME)  
@Target(ElementType.FIELD)  
public @interface Column {  
      
    /** 
     * 字段名称 
     */  
    String value();  
      
    /** 
     * 字段的类型 
     * @return 
     */  
    Class<?> type() default String.class;  
      
    /** 
     * 字段的长度 
     * @return 
     */  
    int length() default 0;  
    
    /*
     * 字段重要性
     * @return
     */
    boolean need() default true;
    
    /*
     * 是否是主键
     * @return
     */
    boolean pk() default false;
    
    /*
     *排序主键
     *@return
     */
    boolean order() default false;
    
    /*
     *判断时候是外键
     *@return 
     */
    boolean foreign() default false;
    
    /*
     *判断是否需要连接属性 
     *@return
     */
    boolean join() default false;
}
