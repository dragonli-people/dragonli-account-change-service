package org.dragonli.service.modules.accountchangeservice.service;

import com.alibaba.dubbo.config.annotation.Service;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.dragonli.service.modules.account.interfaces.AccountChangeService;
import org.dragonli.service.modules.accountchangeservice.AccountChangeVars;
import org.dragonli.service.modules.accountservice.entity.enums.AccountAssetsRecordStatus;
import org.dragonli.service.modules.accountservice.entity.models.AccountAssetsRecordEntity;
import org.dragonli.service.modules.accountservice.entity.models.AccountEntity;
import org.dragonli.service.modules.accountservice.entity.models.AssetEntity;
import org.dragonli.service.modules.accountservice.entity.models.FundFlowEvidenceEntity;
import org.dragonli.service.modules.accountservice.repository.AccountAssetsRecordRepository;
import org.dragonli.service.modules.accountservice.repository.AccountsRepository;
import org.dragonli.service.modules.accountservice.repository.AssetRepository;
import org.dragonli.service.modules.accountservice.repository.FundFlowEvidenceRepository;
import org.redisson.api.RBatch;
import org.redisson.api.RQueue;
import org.redisson.api.RQueueAsync;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service(interfaceClass = AccountChangeService.class, register = true, timeout = 150000000, retries = -1, delay = -1)
public class AccountChangeServiceImpl implements AccountChangeService {
    final Logger logger = LoggerFactory.getLogger(getClass());

    protected static final Map<Long,Long> locks = new ConcurrentHashMap<>();

    /**
     * 是否暂停新队列进入
     */
    private static volatile boolean pauseBefore = false;
    /**
     * 暂停已在redis队列中的处理
     */
    private static volatile boolean pause = false;
    private final ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
    private final Map<Integer, Runnable> tickThreads = new ConcurrentHashMap<>();
    private final AtomicInteger cacheBeforeSaveCount = new AtomicInteger(0);
    private final Queue<AccountAssetsRecordEntity> cacheBeforeSave = new ConcurrentLinkedQueue<>();

    @Value("${cacheListKey:cacheListKey}")
    private String cacheListKey;

    @Autowired
    private RedissonClient redissonClient;
    @Autowired
    private ChangeAccountService changeAccountService;
    @Autowired
    AccountAssetsRecordRepository accountAssetsRecordRepository;
    @Autowired
    AccountsRepository accountsRepository;
    @Autowired
    AssetRepository assetRepository;
    @Autowired
    FundFlowEvidenceRepository fundFlowEvidenceRepository;

    @PostConstruct
    public void start() {

        if (lastId == null) {
//			String sql = "SELECT MIN(id) FROM account_assets_record WHERE recordStatus = 'INIT';";
            AccountAssetsRecordEntity queryForInt = accountAssetsRecordRepository.findFirstByRecordStatusOrderById(
                    AccountAssetsRecordStatus.INIT);
//			OptionalInt queryForInt = assetDb.queryForInt(sql);
//			lastId = queryForInt.isPresent() ? queryForInt.getAsInt() - 1 : 0;
            lastId = (queryForInt != null) ? queryForInt.getId() - 1 : 0;
        }

        reInitTicks();

        //启动数据库轮询进程
        cachedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        tick3();
                    } catch (Exception e) {
                        logger.error("====tick3 err", e);
                        try {
                            //TODO 发短信，警告
//							otherService.sendWarningMessage("AccountAssetsQueueService tick3 error:"+e.getMessage());
                        } catch (Exception e2) {
                            logger.error("sendWarningMessage error", e2);
                        }
                    }
                    try {
                        Thread.sleep(8L);
                    } catch (InterruptedException e) {
                        logger.error("tick3 InterruptedException", e);
                    }
                }
            }
        });
    }

    @Value("${crypto.ui.smallTimeout}")
    public int evidenceTimeout;

    @Override
    @Transactional
    public Boolean addChangeRecord(Long evidenceId) throws Exception{
        FundFlowEvidenceEntity evidenceEntity = fundFlowEvidenceRepository.get(evidenceId);
        if(null == evidenceEntity)return false;
        AccountAssetsRecordEntity accountAssetsRecordEntity =
                accountAssetsRecordRepository.findFirstByOrderId(evidenceEntity.getOrderId());
        if(accountAssetsRecordEntity != null) return false;
        return addChangeRecord(evidenceEntity);
    }

    /**
     * 增加一条账户变更记录
     * @param evidence
     * @return Boolean
     * @throws Exception
     */
    @Transactional
    public Boolean addChangeRecord( FundFlowEvidenceEntity evidence) throws Exception {
        if (evidence == null) throw new Exception("cant all be null");
//		evidence.setTimeout(System.currentTimeMillis() + evidenceTimeout);
        AccountEntity account = null;

        AccountAssetsRecordEntity record = new AccountAssetsRecordEntity();
        record.setAccountId(evidence.getAccountId());
        record.setUserId(evidence.getUserId());
        record.setFlowAmount(evidence.getFlowAmount());
        record.setFlowTypeRecord(evidence.getFlowType());
        long accountId = record.getAccountId() != 0 ? record.getAccountId() : evidence.getAccountId();
        account = accountsRepository.getOne(accountId);

        record.setGroupId((int) (evidence.getAccountId() % AccountChangeVars.groupCount));
        record.setAccountId(account.getId());
        record.setUserId(evidence.getUserId());
        record.setRecordStatus(AccountAssetsRecordStatus.INIT);
        record.setBeforeBalance(account.getBalance());
        record.setAfterBalance(account.getBalance());//.add(evidence.getFlowAmount()));
        record.setBeforeFrozen(account.getFrozen());
        record.setAfterFrozen(account.getFrozen());//.subtract(evidence.getFlowAmount()));
        record.setEvidenceId(evidence.getId());
        record.setCurrency(evidence.getCurrency());
        record.setAccountVersion(account.getAccountVersion());
        record.setRecordStatus(AccountAssetsRecordStatus.INIT);
        record.setOrderId(evidence.getOrderId());
        AssetEntity ass = assetRepository.findByCode(evidence.getCurrency());
        record.setAssetId(ass.getId());

        record.setRemark(record.getRemark() == null ? "" : record.getRemark().trim());

        //之前的随机数机制，可改为t.getId()
//
//		if(r.getGroupId() == null)
//			data.setGroupId( 0 );
        RQueue<String> queue = redissonClient.getQueue(cacheListKey + record.getGroupId());
        queue.add(JSON.toJSONString(record));
        return true;
    }

    /**
     * 按每个分组分别给予一个线程去tick 每个tick绑定一个groupId(余数个位数)
     */
    private void reInitTicks() {

        AccountChangeVars.groups.forEach(remainderId -> {
            if (!tickThreads.containsKey(remainderId)) {
                // 按groupId，每个id启动一个轮询redis的进程
                Runnable one = new Runnable() {
                    @Override
                    public void run() {
                        while (tickThreads.containsKey(remainderId))// 如果map中已经不包含我，则我已不必再执行
                        {
                            try {
                                tick1(remainderId);
                            } catch (Exception e) {
                                logger.error("==tick1 err==", e);
                                try {
                                    // 短信警告
//									otherService.sendWarningMessage("AccountAssetsQueueService tick1 error:"+e
//									.getMessage());
                                } catch (Exception e2) {
                                    logger.error("sendWarningMessage error", e2);
                                }
                            }
                            try {
                                Thread.sleep(8L);
                            } catch (InterruptedException e) {
                                logger.error("tick1 InterruptedException", e);
                            }
                        }
                    }
                };
                tickThreads.put(remainderId, one);
                cachedThreadPool.execute(one);
            }
        });
    }

    /**
     * 从redis队列里取出新的，准备执行批量写入。这个函数是按groupId单线程的，需要在start中按groupId启动
     * @throws Exception
     */
    public void tick1(int groupId) throws Exception {
        if (pauseBefore) {
            return;
        }
//		RList<String> cacheList = redissonClient.getList(cacheListKey+groupId);
        //lpush cacheListKey1 "\{\"id\":\"1\",\"order_id\":\"10000\"\}"
        RQueue<String> queue = redissonClient.getQueue(cacheListKey + groupId);
        int len = Math.min(queue.size(), 1000);//config
        if (len == 0) {
            return;
        }

        //TODO batch失效
        RBatch batch = redissonClient.createBatch();
        RQueueAsync<String> batchQueue = batch.getQueue(cacheListKey + groupId);
        for (int i = 0; i < len; i++)
            batchQueue.pollAsync();
        List<?> result = batch.execute();

        final List<JSONObject> dataArr = result.stream().map(obj -> JSON.parseObject(obj.toString())).collect(
                Collectors.toList());
        cachedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    tick2(dataArr);
                } catch (Exception e) {
                    logger.error("tick2 err:", e);
                }
            }
        });

        //TODO 临时解决方案
//		final List<JSONObject> dataArr = queue.parallelStream().map(obj -> JSON.parseObject(obj.toString())).collect
//		(Collectors.toList());
//		cachedThreadPool.execute(new Runnable() {
//			@Override
//			public void run() {
//				try {
//					tick2(dataArr);
//				} catch (Exception e) {
//					logger.error("tick2 err:",e);
//				}
//			}
//		});

    }

    /**
     * 批量写入内存里的并发LinkedList，这个函数是多线程执行的，由tick1启动线程触发，无需在start中启动
     * @param dataArr
     * @throws Exception
     */
    public void tick2(List<JSONObject> dataArr) throws Exception {
        if (dataArr.size() == 0) return;
        dataArr.stream().forEach(one -> cacheBeforeSave.add(one.toJavaObject(AccountAssetsRecordEntity.class)));
        cacheBeforeSaveCount.addAndGet(dataArr.size());
    }

    /**
     * 正在处理的record记录，key:record id
     */
    private final Map<Long, Boolean> handlingKeys = new ConcurrentHashMap<>();
    /**
     * ??
     */
    private final Queue<AccountAssetsRecordEntity> allRecords = new LinkedList<>();
    //	private volatile Integer[] allGroups = {};
    private final Set<Long> set = new HashSet<>();
    private volatile Long lastId = 0L;
    private boolean tick3HadFirstReadDb = false;
    /**
     * 不停从数据库中查出新的，这个函数是单线程执行的。需要在start中启动
     * @throws Exception
     */
    public void tick3() throws Exception {
        if (pause) return;
        //从内存里的并发LinkedList里取出，并批量入库
        int len = Math.min(1000, cacheBeforeSaveCount.get());
//        if (len == 0) {
//            return;
//        }
        Long oldLastId = null;
        //刚启动时要读取数据库
        if(len != 0 || !tick3HadFirstReadDb) {
            try{
                List<AccountAssetsRecordEntity> saves = new LinkedList<>();
                for (int i = 0; i < len; i++)
                    saves.add(cacheBeforeSave.poll());
                logger.info("tick3 before-saves:" +
                        saves.stream().map(AccountAssetsRecordEntity::getEvidenceId).map(Object::toString).collect(
                                Collectors.joining(",")));
                logger.info("tick3 before-saves uuid:" +
                        saves.stream().map(AccountAssetsRecordEntity::getOrderId).map(Object::toString).collect(
                                Collectors.joining(",")));
                //		assetDb.save(saves);

                //		accountAssetsRecordRepository.saveAll(saves);
                logger.info("tick3  saves : " +
                        saves.stream().map(AccountAssetsRecordEntity::getEvidenceId).map(Object::toString).collect(
                                Collectors.joining(",")));
                logger.info("tick3  saves : " +
                        saves.stream().map(AccountAssetsRecordEntity::getOrderId).map(Object::toString).collect(
                                Collectors.joining(",")));
                //将启用一个新事务，批量写入！！！此步操作极其重要！！
                if(len!=0) {
                    changeAccountService.saveAssetsRecord(saves);
                    cacheBeforeSaveCount.addAndGet(-len);
                }
                oldLastId = lastId;

                //将启用一个新事务，批量读取！！！此步操作极其重要！！
                List<AccountAssetsRecordEntity> recordList = changeAccountService.getRecordList(lastId);
                logger.info("tick3 batch get : " +
                        recordList.stream().map(AccountAssetsRecordEntity::getEvidenceId).map(Object::toString).collect(
                                Collectors.joining(",")));
                logger.info("tick3 batch get : " +
                        recordList.stream().map(AccountAssetsRecordEntity::getOrderId).map(Object::toString).collect(
                                Collectors.joining(",")));
                recordList.stream().filter(record -> AccountChangeVars.groups.contains(record.getGroupId()) &&
                        record.getRecordStatus().equals(AccountAssetsRecordStatus.INIT) &&
                        !handlingKeys.containsKey(record.getId())
                ).forEach(record -> {
                    handlingKeys.put(record.getId(), true);//此锁是为了保障不重复从数据库里添加
                    allRecords.add(record);
                });
                tick3HadFirstReadDb = true;
                long recordMaxId = recordList.size() == 0 ? 0 :
                        recordList.stream().max(Comparator.comparing(AccountAssetsRecordEntity::getId)).get().getId();

                logger.info("tick3 recordMaxId" + recordMaxId);
                logger.info("tick3 lastId======>last lastId:" + lastId);
                lastId = Math.max(lastId, recordMaxId);
                logger.info("tick3 ====2====> saves :current lastId: " + lastId);
            }catch (Exception e ){
                //之前的容错，其实可以抛弃？最后一句还能如何异常?
//                if(oldLastId != null)
//                    lastId = oldLastId;
                logger.error("AccountAssetsQueueService.tick3.read from db.err===", e);
                try {
                    //TODO 短信通知
                } catch (Exception e2) {
                    logger.error("sendWarningMessage error", e2);
                }
            }
        }

        if (allRecords.size() == 0) return;//即使len为0，allRecords里也有可能积蓄了一些待执行的
        Set<Long> currentLocks = new HashSet<>();
        boolean beginExecute = false;
        //批量取出Record表里的数据
        //TODO 需要改成jap的方式
        try {
            @SuppressWarnings("unused") long t1 = System.currentTimeMillis();

            logger.info("tick3 after filter : " +
                    allRecords.stream().map(AccountAssetsRecordEntity::getEvidenceId).map(Object::toString).collect(
                            Collectors.joining(",")));
            logger.info("tick3 after filter : " +
                    allRecords.stream().map(AccountAssetsRecordEntity::getOrderId).map(Object::toString).collect(
                            Collectors.joining(",")));

            Queue<AccountAssetsRecordEntity> tempQueueLocking = new LinkedList<>();//,lastTime-10000
            AccountAssetsRecordEntity one = null;

            Queue<AccountAssetsRecordEntity> tempQueue = new LinkedList<>();//,lastTime-10000
            while ((one = allRecords.poll()) != null) {
                if (locks.containsKey(one.getAccountId()))//如果有锁，即在处理中（理论上说，撞车概率很小，除非用户使用机器人等疯抢）
                {
                    logger.info("tick 3 :tempQueueLocking add:"+one.getOrderId());
                    tempQueueLocking.add(one);
                } else {
                    //给这个accountId上锁
                    currentLocks.add(one.getAccountId());
                    locks.put(one.getAccountId(), System.currentTimeMillis());
                    tempQueue.add(one);
                    logger.info("tick 3:tempQueue[will exec] add:"+one.getOrderId());
                }
            }
            logger.info("tick3 tempQueueLocking======>1");
            allRecords.addAll(tempQueueLocking);//上锁的扔回这个队列
            beginExecute = true;

//			AccountAssetsRecord r,Tickertape t
            tempQueue.forEach(record -> {
                cachedThreadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            changeAccountService.tick4(record.getId(),record.getOrderId());//将启用一个新事务
                        } catch (Exception e) {
                            //todo 重试处理
                            logger.info("tick4 err："+record.getEvidenceId()+"||"+record.getOrderId());
                            logger.error("tick4 err："+record.getEvidenceId()+"||"+record.getOrderId(), e);
                        } finally {
                            if (record != null){
                                locks.remove(record.getAccountId());
                                handlingKeys.remove(record.getId());
                            }

                        }
                    }
                });
            });
        } catch (Exception e) {
            //如果未走到线程执行的地方就异常了，释放本次(!)tick加上的锁
            //虽然想不到beginExecute=true之前的代码为何会异常
            if(!beginExecute){
                for (Long id : currentLocks)
                    locks.remove(id);
            }
            logger.error("AccountAssetsQueueService.tick3.execute.err===", e);
        }
    }
}
