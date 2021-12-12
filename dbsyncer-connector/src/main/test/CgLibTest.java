import org.dbsyncer.common.util.JsonUtil;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.cglib.beans.BeanGenerator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author AE86
 * @version 1.0.0
 * @date 2021/12/12 15:45
 */
public class CgLibTest {

    @Test
    public void testCgLib() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        BeanGenerator beanGenerator = new BeanGenerator();
        beanGenerator.addProperty("value", String.class);

        Object myBean = beanGenerator.create();
        Method setter = myBean.getClass().getMethod("setValue", String.class);
        setter.invoke(myBean, "Hello cglib");

        Method getter = myBean.getClass().getMethod("getValue");
        Assert.assertEquals("Hello cglib", getter.invoke(myBean));

        String json = JsonUtil.objToJson(myBean);
        System.out.println(json);
    }
}
