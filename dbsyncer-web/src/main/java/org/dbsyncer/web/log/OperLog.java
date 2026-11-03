/**
 * DBSyncer Copyright 2020-2026 All Rights Reserved.
 */
package org.dbsyncer.web.log;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 操作日志：标注在 Web 变更接口上，由 {@link OperLogInterceptor} 统一落库。
 * <p>
 * {@code type} 使用 {@link OperLogType} 常量，对应 {@link org.dbsyncer.parser.LogType} 类型码。
 *
 * @author wuji
 * @version 1.0.0
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OperLog {

    /**
     * LogType 类型码，见 {@link OperLogType}
     */
    String type();

    /**
     * 列表摘要；空则用「动作+模块名」，若有请求参数 name 则追加 {@code :name}
     */
    String message() default "";
}
