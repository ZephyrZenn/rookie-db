package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

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
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        if (LockType.substitutable(explicitLockType, requestType)) {
            return;
        }

        // 如果当前隐式的锁类型已经满足需要，就不做任何操作
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }

        // 确保父级已经获取了前置锁
        if (parentContext != null) {
            LockType parentLock = LockType.parentLock(requestType);
            ensureAncestorLock(parentContext, parentLock, transaction);
        }

        if (explicitLockType == LockType.IX && requestType == LockType.S) {
            lockContext.promote(transaction, LockType.SIX);
            return;
        }

        if (explicitLockType.isIntent()) {
            lockContext.escalate(transaction);
            return;
        }

        if (explicitLockType == LockType.NL) {
            lockContext.acquire(transaction, requestType);
        } else {
            lockContext.promote(transaction, requestType);
        }

        // TODO(proj4_part2): implement
        return;
    }

    // TODO(proj4_part2) add any helper methods you want
    private static void ensureAncestorLock(LockContext parent, LockType requestType, TransactionContext transaction) {
        if (parent == null || requestType == null) return;
        LockType effectiveLockType = parent.getEffectiveLockType(transaction);
        LockType explicitLockType = parent.getExplicitLockType(transaction);
        if (LockType.substitutable(explicitLockType, requestType) || LockType.substitutable(effectiveLockType, requestType))
            return;
        LockContext grandContext = parent.parentContext();
        LockType grandLock = LockType.parentLock(requestType);
        ensureAncestorLock(grandContext, grandLock, transaction);
        if (explicitLockType == LockType.NL) {
            parent.acquire(transaction, requestType);
        } else {
            parent.promote(transaction, requestType);
        }
    }
}
