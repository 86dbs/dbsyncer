package org.dbsyncer.schedule.quartz;

import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * 驱动定时任务执行工厂
 *
 * @author AE86
 * @ClassName: TaskQuartzJob
 * @Description: 触发驱动定时抽取数据, 暂时只支持Mysql、Oracle、Ldap
 * @date: 2017年11月8日 下午4:17:43
 */
public class TaskQuartzJob implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobDetail jobDetail = context.getJobDetail();
        // 获取任务ID
        String taskId = jobDetail.getKey().getName();
        // 即将面临的问题如下：
        // 1.如果上一次任务未处理完成,而当前又产生了新的任务,此时多个任务可能会在同一时间执行相同工作,会造成重复数据.
        // 2.如果上一次任务未处理完成,而当前又产生了新的任务,长时间下去,可能会造成任务堆积,影响系统性能.

        // 相应的解决方法：
        // 1.创建集合,存放执行的任务
        // 2.从集合里检查上一次任务存在,继续执行.
        // 3.从集合里检查上一次任务不存在,拒绝当前任务.
        //TaskQuartzHandle.getInstance().handle(taskId);
    }

}
