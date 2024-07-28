package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        // 若当前上下文是只读，就说明不支持后续操作
        if(this.readonly)
            throw new UnsupportedOperationException("Read-only lock");
        // 判断是否有效，即父锁是否支持这个类型的子锁
        if(parent != null) {
            LockType type = this.parentContext().getExplicitLockType(transaction);
            if (!LockType.canBeParentLock(type, lockType))
                throw new InvalidLockException("Invalid lock type: " + lockType);
            // 如果祖先有SIX锁，我们就禁止获取 IS/S 锁，并将其视为无效请求。
            if(hasSIXAncestor(transaction) && (lockType.equals(LockType.IS) || lockType.equals(LockType.S)))
                throw new InvalidLockException("Lock acquisition limit exceeded");
            // 如果lock是NL锁，那么就需要释放父的锁
            if(lockType.equals(LockType.NL)) {
                if(!parentContext().getExplicitLockType(transaction).equals(LockType.NL))
                    parentContext().release(transaction);
                return;
            }
        }
        // 判断是否已经授予过该锁
        if(this.lockman.getLocks(transaction).contains(new Lock(name, lockType, transaction.getTransNum())))
            throw new DuplicateLockRequestException("Duplicate lock type: " + lockType);
        // 正式上锁
        LockType type = getExplicitLockType(transaction);
        if(type.isIntent() && LockType.canBeParentLock(type, lockType)) {
            List<ResourceName> resourceNames = sisDescendants(transaction);
            if(!resourceNames.contains(name)) resourceNames.add(name);
            lockman.acquireAndRelease(transaction, name, lockType, resourceNames);
        } else
            lockman.acquire(transaction, name, lockType);
        if(parent != null)
            parentContext().numChildLocks.put(transaction.getTransNum(), parentContext().numChildLocks.getOrDefault(transaction.getTransNum(), 0) + 1);
        return;
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        // 若当前上下文是只读，就说明不支持后续操作
        if(this.readonly)
            throw new UnsupportedOperationException("Read-only lock");
        // 如果事务并没有资源的锁
        List<Lock> locks = lockman.getLocks(transaction);
        Lock lock = null;
        if(!locks.isEmpty()) {
            // 若在要释放的资源上未找到该事务的锁，抛出异常
            Boolean isFind = false;
            for (Lock lock1 : locks) {
                if(lock1.name.equals(this.name)) {
                    isFind = true;
                    lock = lock1;
                }
            }
            if (!isFind)
                throw new NoLockHeldException("No lock found for resource " + name);
        }
        // 如果锁释放会违反多重粒度锁定约束
        // 找到孩子节点中对应的锁
        if(!children.isEmpty()) {
            LockContext child = childContext(String.valueOf(name));
            if (!LockType.substitutable(getExplicitLockType(transaction), child.getExplicitLockType(transaction))) //!child.getExplicitLockType(transaction).equals(LockType.NL) &&
                throw new InvalidLockException("Invalid lock type: " + getExplicitLockType(transaction));
        }
        lockman.release(transaction, name);
        if(parent != null)
            parentContext().numChildLocks.put(transaction.getTransNum(), parentContext().numChildLocks.get(transaction.getTransNum()) - 1);
        return;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        // 若当前上下文是只读，就说明不支持后续操作
        if(this.readonly)
            throw new UnsupportedOperationException("Read-only lock");
        // 如果事务没有锁可以升级
        if(lockman.getLocks(transaction).isEmpty())
            throw new NoLockHeldException("No lock found for resource " + name);
        // 判断升级的锁与原锁之间是否一致
        if(parent != null) {
            // 升级的锁不能与父锁起冲突
            LockType type = parentContext().getExplicitLockType(transaction);
            if (!LockType.canBeParentLock(type, newLockType))
                throw new InvalidLockException("Invalid lock type: " + newLockType);
        }
        LockType type = getExplicitLockType(transaction);
        if (type.equals(newLockType) || !LockType.substitutable(newLockType, type) ||
                (newLockType.equals(LockType.SIX) && !(type.equals(LockType.IX) || type.equals(LockType.IS) || type.equals(LockType.S))))
            throw new InvalidLockException("Invalid lock type: " + newLockType);

        // 升级锁，并释放S/IS类型的后代锁
        lockman.promote(transaction, name, newLockType);
        if(newLockType.equals(LockType.SIX) && (type.equals(LockType.IS) || type.equals(LockType.IX))) {
            List<ResourceName> resourceNames = sisDescendants(transaction);
            for (ResourceName resourceName : resourceNames) {
                lockman.release(transaction, resourceName);
            }
        }

        return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        // 若当前上下文是只读，就说明不支持后续操作
        if(this.readonly)
            throw new UnsupportedOperationException("Read-only lock");
        // 如果事务没有锁
        if(lockman.getLocks(transaction).isEmpty())
            throw new NoLockHeldException("No lock found for resource " + name);

        // 若已经升级过了
//        if(numChildLocks.get(transaction.getTransNum()) == null || numChildLocks.get(transaction.getTransNum()) == 0)
//            return;
        // 更新当前锁为升级的锁
        LockType type = getExplicitLockType(transaction);
        if(type.equals(LockType.IX))
            type = LockType.X;
        else if(type.equals(LockType.IS))
            type = LockType.S;
        else
            return;
        // 删除所有的子锁
//        if(getNumChildren(transaction) != 0) {
//            for (Map.Entry<String, LockContext> entry : children.entrySet()) {
//                if (!entry.getValue().lockman.getLocks(entry.getValue().name).isEmpty()) {
//                    entry.getValue().lockman.release(transaction, entry.getValue().name);
//                    children.remove(entry.getKey());
//                }
//            }
//        }
//        lockman.promote(transaction, name, type);
        List<ResourceName> resourceNames = sisDescendants(transaction);
        lockman.acquireAndRelease(transaction, name, type, resourceNames);
        numChildLocks.remove(transaction.getTransNum());
        return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
//        List<Lock> locks = lockman.getLocks(transaction);
//        Lock lock = null;
//        if (!locks.isEmpty()) {
//            for (Lock lock1 : locks) {
//                if (lock1.name.equals(this.name)) {
//                    return lock1.lockType;
//                }
//            }
//        }
//        if(parent == null)
//            return LockType.NL;
        return lockman.getLockType(transaction, name);
//        return LockType.NL;
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        if(parent == null) {
            List<Lock> locks = lockman.getLocks(transaction);
            for(Lock lock : locks) {
                if(lock.lockType.equals(LockType.S))
                    return lock.lockType;
            }
        }
        else
            return parent.getEffectiveLockType(transaction);
        return LockType.NL;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<Lock> locks = parentContext().lockman.getLocks(transaction);
        for (Lock lock : locks) {
            if(lock.lockType.equals(LockType.SIX))
                return true;
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> name = new ArrayList<>();
        LockContext childContext = childContext(name.toString());
        List<Lock> locks = childContext.lockman.getLocks(transaction);
        for (Lock lock : locks) {
            if (lock.name.isDescendantOf(this.name) || lock.name.equals(this.name))
                if (lock.lockType.equals(LockType.IS) || lock.lockType.equals(LockType.S)) {
                    name.add(lock.name);
                }
        }
        return name;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

