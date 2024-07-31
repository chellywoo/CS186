package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.List;
import java.util.Stack;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        // 获取显式锁、隐式锁
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        // 获取当前事务的锁
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        if (LockType.substitutable(explicitLockType, requestType)) return;

        // 如果已经授予了这个锁，就不需要重复授予了
        if(explicitLockType.equals(requestType))
            return;
        /**
         * 判断当前事务是否需要对上层进行授予锁等操作
         */
        // 1. 若当前是数据库的事务，那就只需要判断是否已经上锁，但在测例里并没有看到直接调用数据库上锁
        if(parentContext == null) {
            if(LockType.substitutable(explicitLockType, requestType))
                lockContext.promote(transaction, requestType);
            else
                lockContext.acquire(transaction, requestType);
            return;
        }
        // 2. 若不是数据库的上下文，是表或者页的，需要先得到上层上下文进行判断
        Stack<LockContext> stack = ensureSufficient(lockContext);
        // 2.1 如果当前请求的锁是NL类型，那么就直接授予，因为在lockcontext中有检查如果授予的是NL的话，就会将上层lockcontext中的锁释放，这里不需要进行二次判断了
        if(requestType == LockType.NL) {
//            while(!stack.isEmpty()) {
//                if(stack.peek().getEffectiveLockType(transaction) != LockType.NL)
//                   stack.pop().release(transaction);
//                else
//                    return;
//            }
            lockContext.acquire(transaction, requestType);
            return;
        }
        // 2.2 考虑当前锁是IX的情况，如果申请的锁是X时，需要授予SIX锁
        if(explicitLockType.equals(LockType.IX) && requestType.equals(LockType.S)){
            lockContext.promote(transaction, LockType.SIX);
            return;
        }
        // 2.3 若原来的锁是意向锁，需要进行升级
        if(explicitLockType.isIntent()) {
//            lockContext.release(transaction);
            lockContext.escalate(transaction);
            explicitLockType = lockContext.getExplicitLockType(transaction);
            if (explicitLockType == requestType || explicitLockType == LockType.X) return;
            return;
        }

        // 2.4 如果当前请求的锁不是NL类型，需要做判断
        // 原来的锁、请求锁一定只会有这几种情况：(NL,S),(NL,X),(S,X)
        while (!stack.isEmpty()) {
            LockContext current = stack.pop();
            LockType type = current.getExplicitLockType(transaction);
            // 若父锁不为空，并且不可以作为子锁的父锁，但是可以进行升级
            if (!LockType.substitutable(type, LockType.parentLock(requestType))){
                if(type.equals(LockType.NL)) {
                    current.acquire(transaction, LockType.parentLock(requestType));
                } else
                    current.promote(transaction, LockType.parentLock(requestType));
            }

        }
        if(explicitLockType.equals(LockType.NL))
            lockContext.acquire(transaction, requestType);
        else
            lockContext.promote(transaction, requestType);
        return;


//        // 2.2.4 当前锁可以被原来的锁升级
//        if(LockType.substitutable(requestType, explicitLockType)) {
//            // 需要判断当前锁的父锁是否也拥有S，X锁
//            if(effectiveLockType.equals(requestType)) {
//                ;
//            } else if(effectiveLockType.equals(LockType.parentLock(requestType))) {
//
//            } else {
//                while(!stack.isEmpty())
//                    stack.pop().promote(transaction, LockType.parentLock(requestType));
//            }
////
////            while(!stack.isEmpty()) {
////                LockContext current = stack.pop();
////                LockType type = current.getExplicitLockType(transaction);
////                if(type.equals(explicitLockType))
////                    current.promote(transaction, requestType);
////                else if(!LockType.substitutable(type, requestType)){
////                    current.release(transaction);
////                    current.acquire(transaction, requestType);
////                }
////            }
//            lockContext.promote(transaction, requestType);
//            return;
//        }

//        return;
    }

    // TODO(proj4_part2) add any helper methods you want
    // 确保您对所有祖先都有适当的锁
    public static Stack<LockContext> ensureSufficient(LockContext lockContext) {
        Stack<LockContext> stack = new Stack<>();
        LockContext parent = lockContext;
        while(parent.parentContext() != null) {
            parent = parent.parentContext();
            stack.push(parent);
        }
        return stack;
    }
}
