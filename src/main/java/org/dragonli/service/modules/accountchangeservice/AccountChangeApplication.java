package org.dragonli.service.modules.accountchangeservice;

import com.alibaba.dubbo.config.spring.context.annotation.DubboComponentScan;
import io.swagger.models.auth.In;
import org.apache.log4j.Logger;
import org.dragonli.service.dubbosupport.DubboApplicationBase;
import org.dragonli.tools.redis.RedisConfiguration;
import org.dragonli.tools.redis.redisson.RedisClientBuilder;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class}, scanBasePackages = {"org.dragonli"})
@DubboComponentScan(basePackages = "org.dragonli.service.modules.accountchangeservice.service")
public class AccountChangeApplication extends DubboApplicationBase {
    public AccountChangeApplication(
            @Value("${service.micro-service.account-change-service.application-name}") String applicationName,
            @Value("${service.micro-service.common.registry-address}") String registryAddr,
            @Value("${service.micro-service.account-change-service.protocol-name}") String protocolName,
            @Value("${service.micro-service.account-change-service.protocol-port}") Integer protocolPort,
            @Value("${service.micro-service.account-change-service.scan}") String registryId,
            @Value("${service.micro-service.account-change-service.group}") String group,
            @Value("${service.micro-service.account-change-service.http-port}") int port,
            @Value("${service.micro-service.account-change-service.account-group}") String accountGroup) {
        //super(applicationName, registryAddr, protocolName, protocolPort, registryId, port);
//		super("dubbo-netty", registryAddr, protocolName, 20900, "com.itranswarp.crypto.serviceInterface", 1);
        super(applicationName, registryAddr, protocolName, protocolPort, registryId, port, null,
                group != null && !"".equals(group.trim()) ? group.trim() : null);
        if (accountGroup == null || "".equals(accountGroup = accountGroup.trim())) {
            logger.error("service.micro-service.account-change-service.account-group cant be empty!");
            System.exit(-1);
        }
        if (accountGroup.matches("^\\D+\\d+$")) {
            int count = Integer.parseInt(accountGroup.replaceAll("^\\D+", ""));
            String[] arr = new String[count];
            for (int i = 0; i < arr.length; i++)
                arr[i] = String.valueOf(i);
            accountGroup = Arrays.asList(arr).stream().collect(Collectors.joining(","));
        }
        if(!accountGroup.matches("^\\d+[,\\d]*\\d+$")) {
            logger.error("service.micro-service.account-change-service.account-group must be number and ',' or +${number}");
            System.exit(-1);
        }
        AccountChangeVars.groups.addAll(
                Arrays.asList(accountGroup.split(",")).stream().map(Integer::parseInt).collect(Collectors.toList()));
        AccountChangeVars.groupCount = AccountChangeVars.groups.size();
    }

    @SuppressWarnings(value = "unused")
    final Logger logger = Logger.getLogger(getClass());

    public static void main(String[] args) {
        SpringApplication.run(AccountChangeApplication.class, args);
    }
}
