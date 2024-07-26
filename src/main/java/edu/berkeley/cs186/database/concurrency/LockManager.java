package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.sql.PreparedStatement;
import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         * 检查这个锁与资源上所有的锁是否冲突
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            if(locks.isEmpty())
                return true;
            for(Lock lock : locks) {
                if(!LockType.compatible(lock.lockType, lockType) && lock.transactionNum != except) {
                    return false;
                }else if(lock.transactionNum == except && !LockType.substitutable(lockType, lock.lockType))
                    return false;
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         * 为事务上锁，若已经存在锁了，就更新该锁
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            Long transName = lock.transactionNum;
            // 先判断是否需要更新
            for(Lock lock1 : locks) {
                if(lock1.transactionNum.equals(transName)) {
                    lock1.lockType = lock.lockType;
                    // 更新事务
                    for(Lock lock2 : transactionLocks.get(transName)) {
                        if(lock2.name.equals(lock.name))
                            lock2.lockType = lock.lockType;
                    }
                    return ;
                }
            }
            transactionLocks.putIfAbsent(transName, new ArrayList<>());
            transactionLocks.get(transName).add(lock);
            locks.add(lock);
            return;
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         * 释放锁，处理阻塞队列
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            // 释放锁
            locks.remove(lock);
            transactionLocks.get(lock.transactionNum).remove(lock);

            // 处理队列
            processQueue();
            return;
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if (addFront) waitingQueue.addFirst(request);
            else waitingQueue.addLast(request);
            return;
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         * 处理队列，从第一个锁向后授予，直到不兼容的那个
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();

            // TODO(proj4_part1): implement
            while(requests.hasNext()){
                LockRequest request = requests.next();
                if(checkCompatible(request.lock.lockType, request.transaction.getTransNum())) {
                    grantOrUpdateLock(request.lock);
                    waitingQueue.removeFirst();
                    request.transaction.unblock();
                } else
                    return;
            }
            return;
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         * 获取事务在资源上的锁类型
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for(Lock lock : locks) {
                if(lock.transactionNum.equals(transaction)) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released 资源上已经存在该事务的锁了，且并未释放
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     *
     * 为事务获取资源上的的“lockType”锁，并在一个原子动作中获取锁后释放事务持有的“releaseNames”上的所有锁。
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        // 判断是否存在问题，需要抛出异常
        Lock lock = new Lock(name, lockType, transaction.getTransNum());
        List<Lock> locks = getLocks(transaction);
        // 资源上已经存在该事务的锁了，且并未释放，抛出异常
        if(!locks.isEmpty()) {
            if (locks.contains(lock))
                throw new DuplicateLockRequestException("Duplicate lock request");

            // 若在要释放的资源上未找到该事务的锁，抛出异常
            List<ResourceName> lockNames = new ArrayList<>();
            for (Lock lock1 : locks) {
                lockNames.add(lock1.name);
            }
            for (ResourceName resourceName : releaseNames) {
                if (!lockNames.contains(resourceName))
                    throw new NoLockHeldException("No lock found for resource " + resourceName);
            }
        }
        //获取锁 + 释放事务对于releaseNames的锁
        boolean shouldBlock = false;
        synchronized (this) {
            //判断新上的锁是否与资源原有的锁兼容
            ResourceEntry resourceEntry = getResourceEntry(name);
            // 若不兼容，则将其加入队列首，事务处于阻塞状态，因为在等待资源
            if(!resourceEntry.locks.isEmpty() && !resourceEntry.checkCompatible(lockType, transaction.getTransNum())) {
                shouldBlock = true;
                transaction.prepareBlock();
                LockRequest request = new LockRequest(transaction, lock);
                resourceEntry.waitingQueue.addFirst(request);
            }
            // 若兼容，上锁，将事务上所有该释放的资源释放掉
            if(!shouldBlock){
                resourceEntry.grantOrUpdateLock(lock);
                for(ResourceName resourceName : releaseNames){
                    ResourceEntry resourceEntry1 = getResourceEntry(resourceName);
//                    for(Lock lock1 : resourceEntry1.locks) {
//                        if(lock1.transactionNum == transaction.getTransNum()) {
//                            resourceEntry1.releaseLock(lock1);
//                        }
//                    }
                    // 使用显式迭代器来遍历集合并进行修改
                    Iterator<Lock> iterator = resourceEntry1.locks.iterator();
                    while (iterator.hasNext()) {
                        Lock lock1 = iterator.next();
                        if (lock1.transactionNum == transaction.getTransNum() && lock1.name != name) {
                            iterator.remove();
                            resourceEntry1.releaseLock(lock1);
                        }
                    }
                }
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        ResourceEntry resourceEntry = getResourceEntry(name);
        Lock lock = new Lock(name, lockType, transaction.getTransNum());

        // 检测该事务是否已经有这个资源的锁，若有，抛出异常
        if(!resourceEntry.locks.isEmpty() && resourceEntry.locks.contains(lock))
            throw new DuplicateLockRequestException("Duplicate lock request");

       // 授予锁
        boolean shouldBlock = false;
        synchronized (this) {
            // 如果新锁与资源上另一个事务的锁不兼容，或者如果资源的队列中有其他事务，则该事务将被阻止，请求将被放置在NAME队列的后面
            if(!resourceEntry.checkCompatible(lockType, transaction.getTransNum()) || !resourceEntry.waitingQueue.isEmpty()){
                shouldBlock = true;
                transaction.prepareBlock();
                LockRequest request = new LockRequest(transaction, lock);
                resourceEntry.waitingQueue.addLast(request);
            } else{
                // 上锁
                resourceEntry.grantOrUpdateLock(lock);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * `Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.`
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        // 检测该事务是否有这个资源的锁，若没有，抛出异常
        List<Long> transList = new ArrayList<>();
        for(Lock lock : resourceEntry.locks)
            transList.add(lock.transactionNum);
        if(resourceEntry.locks.isEmpty() || !transList.contains(transaction.getTransNum()))
            throw new NoLockHeldException("No lock found for resource " + name);
        // You may modify any part of this method.
        synchronized (this) {
            LockType lockType = resourceEntry.getTransactionLockType(transaction.getTransNum());
            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            resourceEntry.releaseLock(lock);
            transaction.unblock();
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        // 判断是否已经存在锁，需要抛出异常
        Lock newLock = new Lock(name, newLockType, transaction.getTransNum());
        Lock oldLock = null;
        List<Lock> locks = getLocks(transaction);
        // 资源上已经存在同类型的锁了，且并未释放，抛出异常
        if(locks.isEmpty()) {
            throw new NoLockHeldException("No lock found for resource " + name);
        }
        if(!locks.isEmpty()) {
            if (locks.contains(newLock))
                throw new DuplicateLockRequestException("Duplicate lock request");

            // 若在要修改锁的的资源上未找到该事务的锁，抛出异常
            List<ResourceName> lockNames = new ArrayList<>();
            for (Lock lock1 : locks) {
                if(lock1.transactionNum == transaction.getTransNum() && lock1.name.equals(name))
                    oldLock = lock1;
                lockNames.add(lock1.name);
            }
            if(!lockNames.contains(name))
                throw new NoLockHeldException("No lock found for resource " + name);
        }
        // 如果不能更新，即不兼容升级的情况，抛出异常
        assert oldLock != null;
        if(!LockType.substitutable(newLock.lockType, oldLock.lockType) || newLockType.equals(oldLock.lockType))
            throw new InvalidLockException("Invalid lock type " + newLock.lockType);
        boolean shouldBlock = false;
        synchronized (this) {
            ResourceEntry resourceEntry = getResourceEntry(name);
            // 如果新锁与资源上另一个事务的锁不兼容，或者如果资源的队列中有其他事务，则该事务将被阻止，请求将被放置在NAME队列的后面
            if (!resourceEntry.checkCompatible(newLockType, transaction.getTransNum())) {
                shouldBlock = true;
                transaction.prepareBlock();
                LockRequest request = new LockRequest(transaction, newLock);
                resourceEntry.waitingQueue.addLast(request);
            } else {
                resourceEntry.grantOrUpdateLock(newLock);
//                transaction.prepareBlock();
//                shouldBlock = true;
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        return resourceEntry.getTransactionLockType(transaction.getTransNum());
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
