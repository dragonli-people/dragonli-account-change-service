package org.dragonli.service.modules.accountchangeservice;

import com.alibaba.dubbo.config.annotation.Reference;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.dragonli.service.modules.accountchangeservice.service.ChangeAccountService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = AccountChangeApplication.class)
public class AccountChangeServiceTest extends AbstractTransactionalJUnit4SpringContextTests {
    @Reference
    ChangeAccountService changeAccountService;

    @Test
    @Rollback(false)
    public void generalTest() throws Exception{

        System.out.println("=======================");
    }

//    public  static void main(String[] args){
//        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("bootstrap.yml");
//
//        context.start();
//
//        UserService userService = (UserService) context.getBean(UserService.class);
//        System.out.println(userService==null);
//    }

}