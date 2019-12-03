package org.dragonli.service.modules.accountchangeservice;

import com.alibaba.dubbo.config.spring.context.annotation.DubboComponentScan;
import org.apache.log4j.Logger;
import org.dragonli.service.dubbosupport.DubboApplicationBase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

import java.net.InetAddress;
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
            @Value("${ACCOUNT_GROUP_CONFIG}") String accountGroup) throws Exception {
        //super(applicationName, registryAddr, protocolName, protocolPort, registryId, port);
//		super("dubbo-netty", registryAddr, protocolName, 20900, "com.itranswarp.crypto.serviceInterface", 1);
        super(applicationName, registryAddr, protocolName, protocolPort, registryId, port, null,
                group != null && !"".equals(group.trim()) ? group.trim() : null);
        String ip = InetAddress.getLocalHost().getHostAddress();
//        accountGroup = "len(8)";
        if (accountGroup == null || "".equals(accountGroup = accountGroup.trim())) {
            logger.error("service.micro-service.account-change-service.account-group cant be empty!");
            System.exit(-1);
        }
        if (accountGroup.matches("^\\D+\\d+\\D*$")) {
            int count = Integer.parseInt(accountGroup.replaceAll("^\\D+", "").replaceAll("\\D+$", ""));
            String[] arr = new String[count];
            for (int i = 0; i < arr.length; i++)
                arr[i] = String.valueOf(i);
            accountGroup = Arrays.asList(arr).stream().collect(Collectors.joining(","));
            accountGroup += ":" + ip;
        }
        accountGroup = Arrays.asList(accountGroup.split("\\|")).stream().filter(
                v -> ip.equals(v.split(":")[1])).findFirst().orElseThrow(
                () -> new Exception("account-group cant find item end with:  :" + ip));
        accountGroup = accountGroup.split(":")[0];
        if (!accountGroup.matches("^\\d+[,\\d]*\\d+$")) {
            logger.error(
                    "service.micro-service.account-change-service.account-group must be number and ',' or +${number}");
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
