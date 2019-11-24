package org.dragonli.service.modules.userservice;

import com.alibaba.dubbo.config.annotation.Reference;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.dragonli.service.modules.user.interfaces.UserService;
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
@SpringBootTest(classes = UserServiceApplication.class)
public class UserServiceTest extends AbstractTransactionalJUnit4SpringContextTests {
    @Reference
    UserService userService;

    @Test
    @Rollback(false)
    public void generalTest() throws Exception{

        System.out.println("=======================");
        String username = "test003",passwd = "11111111",code = null;
        Map<String,Object> result = null;
        List<Map<String,Object>> results = null;

        result = userService.registUser(username,passwd);
        System.out.println(JSON.toJSONString(result));
        System.out.println("----------regist end--------\n");

        result = userService.login(username,passwd);
        System.out.println(JSON.toJSONString(result));
        System.out.println("----------login end--------\n");

        result = userService.generateLoginCode(username);
        System.out.println(JSON.toJSONString(result));
        code = (String)result.get("code");
        System.out.println("----------generateLoginCode end--------\n");

        result = userService.loginByCode(username,code);
        System.out.println(JSON.toJSONString(result));
        System.out.println("----------loginByCode end--------\n");

        result = userService.findUser(username);
        JSONObject user = new JSONObject(result);
        System.out.println(JSON.toJSONString(result));
        Long uid = user.getLong("id");
        System.out.println("----------findUser end--------\n");

        result = userService.getUserById(uid);
        System.out.println(JSON.toJSONString(result));
        System.out.println("----------getUserById end--------\n");

        results = userService.getUserList(Arrays.asList(2L,uid));
        System.out.println(JSON.toJSONString(results));
        System.out.println("----------getUserList end--------\n");

        result = userService.generateValidateCodeByUserId(uid);
        code = (String)result.get("code");
        System.out.println(JSON.toJSONString(result));
        System.out.println("----------generateValidateCodeByUserId end--------\n");

        result = userService.resetPasswdByKey(username,code);
        System.out.println(JSON.toJSONString(result));
        System.out.println("----------resetPasswdByKey end--------\n");

        String newEmail = "abc@gmail.com";
        result = userService.generateEmailCodeById(uid,newEmail);
        code = (String)result.get("code");
        System.out.println(JSON.toJSONString(result));
        System.out.println("----------generateEmailCodeById end--------\n");

        result = userService.changeEmail(uid,newEmail,code,true);
        System.out.println(JSON.toJSONString(result));
        System.out.println("----------changeEmail end--------\n");

        String newPhone = "13522880918";
        result = userService.generatePhoneCodeById(uid,newPhone);
        code = (String)result.get("code");
        System.out.println(JSON.toJSONString(result));
        System.out.println("----------generatePhoneCodeById end--------\n");

        result = userService.changePhone(uid,newEmail,code,true);
        System.out.println(JSON.toJSONString(result));
        System.out.println("----------changePhone end--------\n");

//        Map<String, Object> changePassword(Long id, String password, String newpw, String passwdCode,
//                Boolean dontValicodeOld) throws Exception;
//
//
//        Map<String, Object> changeEmail(Long id, String code, String newEmail, Boolean setEmailValidated) throws Exception;
//
//        Map<String, Object> changePhone(Long id, String code, String newPhone, Boolean setPhoneValidated) throws Exception ;
//
//        Map<String, Object> generateValidateCodeByUserId(Long id) throws Exception ;
//
//        Map<String, Object> generateValidateCodeByUserId(String key) throws Exception;
//
//
//        Map<String, Object> generateLoginCode(String key);

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