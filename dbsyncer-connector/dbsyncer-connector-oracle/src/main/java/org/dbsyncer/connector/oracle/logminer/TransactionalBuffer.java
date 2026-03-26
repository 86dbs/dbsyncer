/**
 * DBSyncer Copyright 2020-2023 All Rights Reserved.
 */
package org.dbsyncer.connector.oracle.logminer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author AE86
 * @Version 1.0.0
 * @Date 2023-12-09 20:26
 */
public class TransactionalBuffer {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Map<String, Transaction> transactions;
    private BigInteger lastCommittedScn;

    public TransactionalBuffer() {
        this.transactions = new HashMap<>();
        this.lastCommittedScn = BigInteger.ZERO;
    }

    public interface CommitCallback {

        void execute(BigInteger smallestScn, BigInteger commitScn, int callbackNumber) throws InterruptedException;
    }

    public boolean isEmpty() {
        return this.transactions.isEmpty();
    }

    private BigInteger calculateSmallestScn() {
        return transactions.isEmpty() ? null
                : transactions.values().stream().map(transaction->transaction.firstScn).min(BigInteger::compareTo).orElseThrow(()->new RuntimeException("Cannot calculate smallest SCN"));
    }

    /**
     * 获取缓冲区中最早的未提交事务的SCN
     * @return 最小的firstScn，如果缓冲区为空返回null
     */
    public Long getSmallestScn() {
        BigInteger smallest = calculateSmallestScn();
        return smallest == null ? null : smallest.longValue();
    }

    public void registerCommitCallback(String txId, BigInteger scn, String operationId, CommitCallback callback) {
        transactions.compute(txId, (k, transaction)-> {
            if (transaction == null) {
                transaction = new Transaction(scn);
            }
            // 检查操作是否已注册（防止重复查询导致的重复处理）
            if (transaction.registeredOperations.add(operationId)) {
                transaction.commitCallbacks.add(callback);
                // 更新事务的lastScn
                if (scn.compareTo(transaction.lastScn) > 0) {
                    transaction.lastScn = scn;
                }
            }
            return transaction;
        });
    }

    public boolean commit(String txId, BigInteger commitScn, long committedScn) {
        Transaction transaction = transactions.remove(txId);
        if (transaction == null) {
            // 只有当TransactionalBuffer中没有该事务时，才检查是否已提交过
            // 如果committedScn或lastCommittedScn大于当前commitScn，说明该事务可能已经处理过（重复查询导致）
            if (committedScn > commitScn.longValue() || lastCommittedScn.longValue() > commitScn.longValue()) {
                logger.info("txId {} not found in buffer and already committed (committedScn={}, lastCommittedScn={}, commitScn={}), ignore.", txId, committedScn, lastCommittedScn, commitScn);
                return false;
            }
            // 没有该事务的DML记录，可能是以下正常情况：
            // 1. 事务只修改了系统表或不在监听范围内的表，DML被查询条件过滤掉
            // 2. 事务的DML在startScn之前已经被处理，只剩下COMMIT
            // 3. 空事务（只有BEGIN/COMMIT，没有实际DML操作）
            logger.debug("txId {} not found in TransactionalBuffer. Likely modified tables outside monitoring scope or system tables.", txId);
            return false;
        }

        BigInteger smallestScn = calculateSmallestScn();

        int counter = transaction.commitCallbacks.size();
        for (CommitCallback callback : transaction.commitCallbacks) {
            try {
                callback.execute(smallestScn, commitScn, --counter);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }
        lastCommittedScn = commitScn;
        return true;
    }

    public boolean rollback(String txId) {
        return transactions.remove(txId) != null;
    }

    private static final class Transaction {

        private final BigInteger firstScn;
        private BigInteger lastScn;
        private final List<CommitCallback> commitCallbacks;
        private final Set<String> registeredOperations;

        private Transaction(BigInteger firstScn) {
            this.firstScn = firstScn;
            this.commitCallbacks = new ArrayList<>();
            this.lastScn = firstScn;
            this.registeredOperations = new HashSet<>();
        }

        @Override
        public String toString() {
            return "Transaction{" + "firstScn=" + firstScn + ", lastScn=" + lastScn + '}';
        }
    }
}