package org.dbsyncer.web.controller.databus;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Controller
@RequestMapping("/dataBus")
public class DataBusController {

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @RequestMapping("")
    public String getView(Model model) {
        return "databus/databus.html";
    }

    /**
     * 定时推送消息
     */
    @Scheduled(fixedRate = 1000)
    public void callback() {
        String format = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        messagingTemplate.convertAndSend("/topic/callback", "定时推送消息时间: " + format);
    }

    /**
     * 第一位，表示秒，取值0-59
     * 第二位，表示分，取值0-59
     * 第三位，表示小时，取值0-23
     * 第四位，日期天/日，取值1-31
     * 第五位，日期月份，取值1-12
     * 第六位，星期，取值1-7
     * [秒 分 时 日 月 星期]
     */
    @Scheduled(cron = "0 0 */1 * * ?")
    public void testCron() {
        String format = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        System.out.println("定时任务:" + format);
    }

}