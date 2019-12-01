package org.dragonli.service.modules.accountchangeservice;

import com.alibaba.dubbo.config.annotation.Reference;
import org.dragonli.service.modules.account.interfaces.AccountChangeService;
import org.dragonli.service.modules.accountchangeservice.service.ChangeAccountService;
import org.dragonli.service.modules.accountservice.entity.enums.AccountType;
import org.dragonli.service.modules.accountservice.entity.enums.EvidenceStatus;
import org.dragonli.service.modules.accountservice.entity.models.AccountEntity;
import org.dragonli.service.modules.accountservice.entity.models.AssetEntity;
import org.dragonli.service.modules.accountservice.entity.models.FundFlowEvidenceEntity;
import org.dragonli.service.modules.accountservice.repository.AccountsRepository;
import org.dragonli.service.modules.accountservice.repository.AssetRepository;
import org.dragonli.service.modules.accountservice.repository.FundFlowEvidenceRepository;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.math.BigDecimal;
import java.util.Random;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = AccountChangeApplication.class)
public class AccountChangeServiceTest extends AbstractTransactionalJUnit4SpringContextTests {
    @Reference
    AccountChangeService accountChangeService;

    @Autowired
    AssetRepository assetRepository;

    @Autowired
    AccountsRepository accountsRepository;

    @Autowired
    FundFlowEvidenceRepository evidenceRepository;

    @Autowired
    ChangeAccountService changeAccountService;

    @Test
    @Rollback(false)
    public void generalTest() throws Exception{

        AssetEntity asset = assetRepository.get(1L);
        AccountEntity account = accountsRepository.get(1L);

        FundFlowEvidenceEntity evidence = new FundFlowEvidenceEntity();
        evidence.setAccountId(1L);
        evidence.setBusinessId(0L);
        evidence.setCurrency(asset.getCurrency());
        evidence.setFlowAmount(BigDecimal.ONE);
        evidence.setFlowStatus(EvidenceStatus.INIT);
        evidence.setOrderId("aa-bb-cc-dd-1-"+(new Random()).nextInt(10000));
        evidence.setStep(1);
        evidence.setTimeout(System.currentTimeMillis()+3000);
        evidence.setUserId(1L);
        evidence.setCreatedAt(System.currentTimeMillis());
        evidence.setUpdatedAt(System.currentTimeMillis());
        evidence.setVersion(0);
        evidence = changeAccountService.saveEvidence(evidence);

        accountChangeService.addChangeRecord(evidence.getId());
        System.out.println("======111====== "+evidence.getId());
        Thread.sleep(500000L);
        System.out.println("=====222======= "+evidence.getId());

        new Thread(()->{
            try{
                Thread.sleep(500000L);
            }catch (Exception e){}
        }).start();
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