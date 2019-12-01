package org.dragonli.service.modules.accountchangeservice.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.dragonli.service.modules.accountservice.constants.AccountConstants;
import org.dragonli.service.modules.accountservice.entity.enums.AccountAssetsRecordStatus;
import org.dragonli.service.modules.accountservice.entity.enums.EvidenceStatus;
import org.dragonli.service.modules.accountservice.entity.models.AccountAssetsRecordEntity;
import org.dragonli.service.modules.accountservice.entity.models.AccountEntity;
import org.dragonli.service.modules.accountservice.entity.models.FundFlowEvidenceEntity;
import org.dragonli.service.modules.accountservice.repository.AccountAssetsRecordRepository;
import org.dragonli.service.modules.accountservice.repository.AccountsRepository;
import org.dragonli.service.modules.accountservice.repository.FundFlowEvidenceRepository;
import org.redisson.api.RList;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class ChangeAccountService  {

    final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    AccountAssetsRecordRepository accountAssetsRecordRepository;
    @Autowired
    FundFlowEvidenceRepository fundFlowEvidenceRepository;
    @Autowired
    AccountsRepository accountsRepository;

    @Autowired
    @Qualifier(AccountConstants.ACCOUNT_REDIS)
    RedissonClient accountRedisson;

    @Value("${ACCOUNT_CALL_BACK_REDIS_KEY:"+AccountConstants.DEFAULT_ACCOUNT_CALL_BACK_REDIS_KEY+"}")
    String accountCallBackRedisKey;

    /**
     * 准备accountDic，这个函数是多线程执行的，但同一个account不会并行。由tick3启动，不需要在start中启动
     *
     * @param id
     * @throws Exception
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void tick4(Long id, String orderId) throws Exception {
        //本方法可以与tick5合并
//		FundFlowEvidence evidence = assetDb.fetch(FundFlowEvidence.class, record.evidenceId);//凭条对象
        AccountAssetsRecordEntity record = accountAssetsRecordRepository.getOne(id);
        if (record == null) throw new Exception("tick4 err:record is null:" + id + "||" + orderId);
        if (record.getRecordStatus() != AccountAssetsRecordStatus.INIT) return;
        FundFlowEvidenceEntity evidence = fundFlowEvidenceRepository.getOne(record.getEvidenceId());
//		SpotAccount account = assetDb.fetch(SpotAccount.class, record.accountId);//用户账户对象
        AccountEntity account = accountsRepository.getOne(record.getAccountId());
        logger.info("tick4:begin:account :" + id + "||" +
                (evidence != null ? evidence.getId() + "||" + evidence.getAccountId() : "null") + "||" + orderId);
        try {
            //这一步的时间单独统计
            tick5(record, evidence, account, orderId);//将沿用当前事务
        } catch (Exception e) {
            logger.info("AccountAssetsQueueService.tick5.err===" + "||" + id + "||" + orderId);
            logger.error("AccountAssetsQueueService.tick5.err===" + "||" + id + "||" + orderId, e);
            try {
                //TODO 发短信
//				otherService.sendWarningMessage("AccountAssetsQueueService tick4 error:"+e.getMessage());
            } catch (Exception e2) {
                logger.error("sendWarningMessage error", e2);
            }
        } finally {
//			jedisPoolProxy.returnResource(jedis);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public FundFlowEvidenceEntity saveEvidence(FundFlowEvidenceEntity evidence) throws Exception {
        return fundFlowEvidenceRepository.save(evidence);
    }

    /**
     * 对资金变动进行真正的处理，是多线程的，跟随tick4的线程。因为新开线程是无意义的，被锁着的用户还是锁着的。由tick启动，无需在start中启动
     *
     * @param record
     * @param evidence
     * @param account
     * @throws Exception
     */
    @Transactional
    public void tick5(AccountAssetsRecordEntity record, FundFlowEvidenceEntity evidence, AccountEntity account,
                      String orderId) throws Exception {
        if (record == null || evidence == null || account == null) {
            //这段容错代码提至此处：如果有一项为空，下面根本不该进行
//            System.out.println("record:" + JSON.toJSONString(record));
//            System.out.println("evidence:" + JSON.toJSONString(evidence));
//            System.out.println("account:" + JSON.toJSONString(account));
            logger.info("tick5 null exception:" + orderId + "|" + (record == null ? "null" : record.getId()) + "|" +
                    (evidence == null ? "null" : evidence.getId()) + (account == null ? "null" : account.getId()));
            return;
        }

        @SuppressWarnings("unused") long t1 = System.currentTimeMillis();
        try {
            logger.info("tick5:handleOne :" + "||" +
                    evidence.getId() + "||" + evidence.getAccountId() + "||" + orderId);
            this.handleOne(record, evidence, account, orderId);//沿用当前事务
        } catch (Exception e) {
            logger.info("tick5:handleOne-error :" + "||" +
                    evidence.getId() + "||" + evidence.getAccountId() + "||" + orderId);
            logger.error("tick5:handleOne-error :" + "||" +
                    evidence.getId() + "||" + evidence.getAccountId() + "||" + orderId, e);
            e.printStackTrace();
            try {
                //TODO 发短信
//				otherService.sendWarningMessage("AccountAssetsQueueService tick5 error:"+e.getMessage());
            } catch (Exception e2) {
                logger.error("sendWarningMessage error", e2);
            }
        } finally {
            //AccountAssetsQueueServiceByGroupId的finally里会解锁，这里无需再来一次？

//            if (account != null) locks.remove(account.getId());
//			if(jedis != null)
//				jedis.close();
        }
    }

    //TODO 处理某一条，需要改成
    @Transactional
    public void handleOne(AccountAssetsRecordEntity record, FundFlowEvidenceEntity evidence, AccountEntity account, String orderId) {

        //特殊情况
        if (AccountAssetsRecordStatus.CLOSE.equals(record.getRecordStatus())) return;

        try {
            logger.info("tick5:change :handleOne begin:" + "||" +
                    (evidence != null ? (evidence.getId() + "||" + evidence.getAccountId()) : "null") + "||" + orderId);
            boolean change = this.change(record, evidence, account, orderId);//沿用当前事务

            logger.info("tick5:notify begin :" + "||" + orderId);
            //执行后续的mq TODO
            JSONObject json = new JSONObject();
            json.put("result", change);
            json.put("busId", evidence.getBusinessId());
            json.put("id", evidence.getId());

            //TODO
            String content = json.toJSONString();
            //发送http
//            messageProducer.sendMessage("accountMessage", content);//发送mq
            RList<String> list = accountRedisson.getList(accountCallBackRedisKey);
            boolean add = list.add(content);
            logger.info("tick5:notify end: handleOne finish:" + "||" + evidence.getBusinessId() + " id : " +
                    evidence.getId() + "||" + orderId + "||" + add);
        } catch (Exception e) {
            logger.info("tick5:change error :" + "||" +
                    (evidence != null ? (evidence.getId() + "||" + evidence.getAccountId()) : "null") + "||" + orderId);
            logger.error("tick5:change error :" + "||" +
                            (evidence != null ? (evidence.getId() + "||" + evidence.getAccountId()) : "null") + "||" + orderId,
                    e);
        }
    }

    //直接变更用户的帐户
    @Transactional
    public boolean change(AccountAssetsRecordEntity record, FundFlowEvidenceEntity evidence, AccountEntity account,
                          String orderId) throws Exception {

        logger.info("tick5:change amount begin:" + "||" +
                evidence.getId() + "||" + evidence.getAccountId() + "||" + orderId);
//		System.out.println("change:"+JSON.toJSONString(record));


        //获得当前账户余额
        BigDecimal balance = account.getBalance();
        BigDecimal frozen = account.getFrozen();
//        evidence.setFlowAmount(balance);
//		boolean successed = changeHandler.get(record.accountType)
//				.get(record.currency).get(record.flowType).change(account, record.flowAmount);
        boolean successed = true;
        if (record.getFlowAmount() != null &&
                account.getBalance().add(record.getFlowAmount()).compareTo(BigDecimal.ZERO) == -1) successed = false;
        //冻结及其它
//        if ( record.getFlowTypeRecord() != null && account.getBalance().add(record.getFlowAmount()).compareTo
//        (BigDecimal.ZERO) == -1)
//            successed = false;
        if (successed) {
            BigDecimal res = account.getBalance().add(record.getFlowAmount());
            //冻结及其它
            account.setBalance(res);
        }

//		if(!evidence.currency.equals(account.currency)) {
//			//货币单位不一致 TODO
//			System.out.println("bug evidence:"+JSON.toJSONString(evidence));
//			System.out.println("bug account:"+JSON.toJSONString(account));
//			return false;
//		}
//		//调账
//		FlowType flowType = evidence.flowType;
//		long balance = account.balance;
//		if(FlowType.ADD.equals(flowType)) {
//			account.balance = balance + evidence.flowAmount;
//		}
//		if(FlowType.SUBTRACTION.equals(flowType)) {
//			account.balance = balance > evidence.flowAmount ? balance - evidence.flowAmount : balance;
//		}
//		if( successed )
//			assetDb.update(account);

//		boolean successed = account.balance != balance;

        //修改记录表与凭条表的状态
//		if(record.id == 0) {
//			record = assetDb.from(AccountAssetsRecord.class).where("evidenceId = ?",record.evidenceId).first();//TODO
//			 无id
//		}
        record.setBeforeBalance(balance);
        record.setAfterBalance(account.getBalance());
        record.setBeforeFrozen(frozen);
        record.setAfterFrozen(account.getFrozen());
        record.setRecordStatus(AccountAssetsRecordStatus.CLOSE);
        record.setStatus(successed);
//		assetDb.update(record);
        accountAssetsRecordRepository.save(record);
        logger.info("update record:" + orderId + "||" + record.getId());

        evidence.setTimeout(0L);
        evidence.setFlowStatus(successed ? EvidenceStatus.CLOSE : EvidenceStatus.BALANCE_NOT_ENOUGH);
        fundFlowEvidenceRepository.save(evidence);
        System.out.println("update evidence:" + orderId + "||" + evidence.getId());

//		assetDb.update(account);
        account.setAccountVersion(account.getAccountVersion() + 1);
        accountsRepository.save(account);

        logger.info("tick5:change amount done:" + successed + "||" + orderId + "||" + record.getId());

        //执行后续sql
//		if(successed && StringUtils.isNotBlank(evidence.getSucceedSql()) )
//			assetDb.updateSql(evidence.getSucceedSql());
//		if(!successed && StringUtils.isNotBlank(evidence.getSucceedSql()))
//			assetDb.updateSql(evidence.getErrSql()); 上面更新过了 这里还需要吗？
        return successed;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<AccountAssetsRecordEntity> getRecordList(Long lastId) {
        List<AccountAssetsRecordEntity> recordList =
                accountAssetsRecordRepository.findTop10ByIdGreaterThanAndRecordStatusOrderById(
                        lastId, AccountAssetsRecordStatus.INIT);
        recordList = recordList.stream().map(
                v -> JSON.parseObject(JSON.toJSONString(v), AccountAssetsRecordEntity.class)).collect(Collectors.toList());
        return recordList;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<AccountAssetsRecordEntity> saveAssetsRecord(List<AccountAssetsRecordEntity> saves) {
        List<AccountAssetsRecordEntity> records = accountAssetsRecordRepository.saveAll(saves);
        return records;
    }
}
