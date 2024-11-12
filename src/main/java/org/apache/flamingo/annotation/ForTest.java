package org.apache.flamingo.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The method annotated by this annotation may be deleted at any time, please do not use
 * it!!!
 *
 * @Author venus
 * @Date 2024/11/11
 * @Version 0.0.1
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ ElementType.METHOD, ElementType.TYPE })
public @interface ForTest {

}
