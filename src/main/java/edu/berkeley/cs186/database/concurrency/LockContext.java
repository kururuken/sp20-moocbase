package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.lang.ProcessBuilder.Redirect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.lang.model.util.ElementScanner6;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 * 
 * We can illustrate how the multigranularity locking works
 * Suppose the hierachy is like this
 *      A
 *    /  \
 *   B1  B2
 *  / \
 * C1 C2
 * 
 * Now transaction t1 wants to get a S lock on C2. It needs to get IS for all the anscestors
 * from top to bottom
 *      A(IS(t1))
 *    /          \
 *   B1(IS(t1))  B2
 *  / \
 * C1 C2(S(t1))
 * 
 * Then trasacrtion t2 wants to get a X lock on B1. Similarly, it needs to get a IX lock on A before
 * it gets a X lock on B1. Since IS does not conflict with IX, we can get it directly
 *      A(IS(t1), IX(t2))
 *    /               \
 *   B1(IS(t1))       B2
 *  / \
 * C1 C2(S(t1))
 * 
 * But on resource B, X conflicts with IS, so we need to block until t1 releases its lock on B1
 * (Usually if t1 wants to release S on C2, it will go from bottom to top. It does not make sense 
 * to release only IS on B1 or A, and is not allowed to do so).
 * 
 * Note that this example illustrates that on LockContext level, for a given transaction
 * The granularity constraint only applies to the same tranaction, i.e. we don't need to worry about
 * other transactions. This is because for all the descendents of a given resource, we must have proper lock type
 * on this resource (intent locks). So we only need to check whether the lock type is compatible on this resource.
 * If the lock types of two transactions are compatible on this resources, then all the descendents of this resource
 * must be compatible. 
 * The compatibility on a given resource is gauranteed by lock manager, so in lock context we don't have to worry
 * about other transactions.
 * 
 * Another example can be this. Now we add one more layer to the hierachy.Suppose Transaction t1 has
 * a S lock on D1.
 *        A(IS(t1))
 *      /          \
 *     B1(IS(t1))  B2
 *    /         \
 *   C1(IS(t1)) C2
 *  /        \
 * D1(S(t1)) D2
 * 
 * What if we now want to get(not promote to because IS cannot be promoted to X) a X lock on B1? 
 * This would not be possible because we cannot have duplicate locks on the same resources for a transaction.
 * So we need to release D1 C1 B1 (in order), promote A to IX and then acquire X on B1
 * 
 * From these two examles, we can conclude that for a valid hierachical locking scenerio, there must be a path 
 * from the root all the way to the bottom element, where each lock on the path will be compatible with it's parent, and 
 * therefore compatible with all it's anscestors. So any two lock on this path would be compatible.
 * 
 * This obsevation tells us that to check whether it is valid to acquire to release a lock on a resource, we only have to 
 * check the parent and children of this resource for the SAME TRANSACTION.
 * 
 * Now let's consider promotion. Actually in promotion the only compatibility problem we should consider is with parent.
 * This is because in a valid promotion, children will always be compatible with the lock type that we are promoting to.
 * (Notice that we cannot promote intent lock to real lock and vice versa, also we can only promote S to X and IS to IX).
 * so all the descendent won't be affected if a promotion is valid on this resource. As a result, the only compatibility problem
 * we should consider is with parent.
 * 
 * The only exception to this is SIX, which we will address on on the method.
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional fields/methods as you see fit.

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
    protected final Map<Long, List<Long>> transactionChildren;
    // The number of children that this LockContext has, if it differs from the number of times
    // LockContext#childContext was called with unique parameters: for a table, we do not
    // explicitly create a LockContext for every page (we create them as needed), but
    // the capacity should be the number of pages in the table, so we use this
    // field to override the return value for capacity().
    protected int capacity;

    // You should not modify or use this directly.
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        // this implementation is thread safe, so we don't worry about updating this?
        // the reason why we worry about this is that
        // we need to update this in children process
        // parent does not know that a transaction holds a lock on its children
        // and many children might read or write this map at the same time
        // so we need this ConcurrentHashMap to ensure thread safe
        this.numChildLocks = new ConcurrentHashMap<>(); 
        this.transactionChildren = new ConcurrentHashMap<>();
        this.capacity = -1;
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to NAME from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
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
     * Acquire a LOCKTYPE lock, for transaction TRANSACTION.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by TRANSACTION
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
    throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        checkReadonly();
        checkParentAndChildrenCompatibility(transaction, lockType);

        lockman.acquire(transaction, name, lockType);

        parentAddChildContext(transaction, getContextNum());
        return;
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     * @throws InvalidLockException if the lock cannot be released (because doing so would
     *  violate multigranularity locking constraints)
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
    throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        checkReadonly();
        checkParentAndChildrenCompatibility(transaction, LockType.NL);

        lockman.release(transaction, name);
        parentRemoveChildContext(transaction);
        return;
    }

    /**
     * Promote TRANSACTION's lock to NEWLOCKTYPE. For promotion to SIX from IS/IX/S, all S,
     * IS, and SIX locks on descendants must be simultaneously released. The helper function sisDescendants
     * may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     * 
     * Following comments come from instruction
     * In the special case of promotion to SIX (from IS/IX/S), 
     * you should simultaneously release all descendant locks of type S/IS, 
     * since we disallow having IS/S locks on descendants when a SIX lock is held. 
     * You should also disallow promotion to a SIX lock if an ancestor has SIX, 
     * because this would be redundant.Note: this does still allow for SIX locks to be held under a SIX lock, 
     * in the case of promoting an ancestor to SIX while a descendant holds SIX. This is redundant, 
     * but fixing it is both messy (have to swap all descendant SIX locks with IX locks) 
     * and pointless (you still hold a lock on the descendant anyways), so we just leave it as is.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a NEWLOCKTYPE lock
     * @throws NoLockHeldException if TRANSACTION has no lock
     * @throws InvalidLockException if the requested lock type is not a promotion or promoting
     * would cause the lock manager to enter an invalid state (e.g. IS(parent), X(child)). A promotion
     * from lock type A to lock type B is valid if B is substitutable
     * for A and B is not equal to A, or if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        checkReadonly();
        // no need to check children, see comments on the top
        if (parent != null && !LockType.canBeParentLock(parent.getExplicitLockType(transaction), newLockType))
            throw new InvalidLockException("Invalid parent lock type");
        if (newLockType != LockType.NL) {
            lockman.promote(transaction, name, newLockType);
        } else {
            if (hasSIXAncestor(transaction))
                throw new InvalidLockException("Cannot promote to SIX because it has SIX anscestor");
            List<ResourceName> sisNames = sisDescendants(transaction);
            sisNames.add(name);
            // promotion to SIX should go through acquireAndRelease
            // as it is not allowed to promote to SIX thorugh LockManager.promote()
            lockman.acquireAndRelease(transaction, name, newLockType, sisNames);
        }

        return;
    }

    /**
     * Escalate TRANSACTION's lock from descendants of this context to this level, using either
     * an S or X lock. There should be no descendant locks after this
     * call, and every operation valid on descendants of this context before this call
     * must still be valid. You should only make *one* mutating call to the lock manager,
     * and should only request information about TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *      IX(database) IX(table1) S(table2) S(table1 page3) X(table1 page5)
     * then after table1Context.escalate(transaction) is called, we should have:
     *      IX(database) X(table1) S(table2)
     *
     * You should not make any mutating calls if the locks held by the transaction do not change
     * (such as when you call escalate multiple times in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all relevant contexts, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if TRANSACTION has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        checkReadonly();
        LockType currentType = getExplicitLockType(transaction);
        if (currentType == LockType.NL)
            throw new NoLockHeldException("No lock hold");
        List<LockType> target = new ArrayList<>();
        target.add(LockType.S);
        target.add(LockType.IS);
        target.add(LockType.X);
        target.add(LockType.IX);
        target.add(LockType.SIX);
        List<Pair<ResourceName, LockType>> ret = getAndReleaseChildLocks(transaction, target);
        List<LockType> childTypes = ret.stream().map(x -> x.getSecond()).collect(Collectors.toList());
        childTypes.add(currentType);
        List<ResourceName> childResources = ret.stream().map(x -> x.getFirst()).collect(Collectors.toList());
        childResources.add(name);
        boolean onlySIS = !childTypes
            .stream()
            .anyMatch(x -> x == LockType.IX || x == LockType.X || x == LockType.SIX);
        LockType escalateType = onlySIS ? LockType.S : LockType.X;
        if (childResources.size() == 1 && escalateType == currentType)
            return; // no need to go to lock manager
        lockman.acquireAndRelease(transaction, name, escalateType, childResources);
        return;
    }

    /**
     * Gets the type of lock that the transaction has at this level, either implicitly
     * (e.g. explicit S lock at higher level implies S lock at this level) or explicitly.
     * Returns NL if there is no explicit nor implicit lock.
     * 
     * If current lock type is S or X, then implicit lock type must be the same.
     * 
     * If current lock type is IS or IX, then we can't have S or X as our ancestors.
     * So if we have SIX in ancestors, the implicit lock type is S, otherwise it's explicit lock type
     * 
     * If current lock type is SIX, then implicit lock type must be S, since it's ancestors can only be 
     * IX and SIX.
     * 
     * If current lock type is NL, we look for parent's implicit lock type. if it's intent lock type,
     * then our implicit lock type is NL, otherwise it's parent's implicit lock type.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(proj4_part2): implement
        LockType currentExplicit = getExplicitLockType(transaction);

        if (currentExplicit == LockType.S || currentExplicit == LockType.X)
            return currentExplicit;

        if (currentExplicit == LockType.IS || currentExplicit == LockType.IX) {
            if (hasSIXAncestor(transaction)) {
                return LockType.S;
            }
            return currentExplicit;
        }

        if (currentExplicit == LockType.SIX)
            return LockType.S;

        // currentExplicit == NL
        if (parent == null)
            return LockType.NL;
        LockType parentImplicit = parent.getEffectiveLockType(transaction);
        if (parentImplicit == LockType.S || parentImplicit == LockType.X)
            return parentImplicit;
        return LockType.NL;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        if (parent == null)
            return false;
        if (parent.getExplicitLockType(transaction) == LockType.SIX)
            return true;
        return parent.hasSIXAncestor(transaction);
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or IS and are descendants of current context
     * for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction holds a S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<LockType> target = new ArrayList<>();
        target.add(LockType.S);
        target.add(LockType.IS);
        List<Pair<ResourceName, LockType>> ret = getAndReleaseChildLocks(transaction, target);
        return ret.stream().map(x -> x.getFirst()).collect(Collectors.toList()); // 
    }

    private List<Pair<ResourceName, LockType>> getAndReleaseChildLocks(TransactionContext transaction, List<LockType> targetLockTypes) {
        List<Pair<ResourceName, LockType>> ret = new ArrayList<>();
        long transactionNum = transaction.getTransNum();
        List<Long> childContexts = new ArrayList<>(transactionChildren.getOrDefault(transactionNum, new ArrayList<>())); // make a copy to prevent concurrent read/write exception
        for (Long childContextNum : childContexts) {
            LockContext childContext = childContext(childContextNum);
            LockType childLockType = childContext.getExplicitLockType(transaction);
            // Note that we remove childContext from transaction's childContext list
            // before we actually release these locks. So the context and locks are
            // not synchronized at this time.
            // This should not be a problem because lock contexts are only related to 
            // certain transaction and has nothing to do with other transactions.
            if (targetLockTypes.contains(childLockType)) {
                Pair<ResourceName, LockType> pair = new Pair<>(childContext.name, childLockType);
                ret.add(pair);
                childContext.parentRemoveChildContext(transaction);
            }
            ret.addAll(childContext.getAndReleaseChildLocks(transaction, targetLockTypes));
        }
        return ret;
    }

    /**
     * Get the type of lock that TRANSACTION holds at this level, or NL if no lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, name);
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
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
     * Gets the context for the child with name NAME (with a readable version READABLE).
     */
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                                           this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name NAME.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
    }

    /**
     * Sets the capacity (number of children).
     */
    public synchronized void capacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Gets the capacity. Defaults to number of child contexts if never explicitly set.
     */
    public synchronized int capacity() {
        return this.capacity < 0 ? this.children.size() : this.capacity;
    }

    /**
     * Gets the saturation (number of locks held on children / number of children) for
     * a single transaction. Saturation is 0 if number of children is 0.
     */
    public double saturation(TransactionContext transaction) {
        if (transaction == null || capacity() == 0) {
            return 0.0;
        }
        return ((double) numChildLocks.getOrDefault(transaction.getTransNum(), 0)) / capacity();
    }

    private void checkParentAndChildrenCompatibility(TransactionContext transaction, LockType lockType)
    throws InvalidLockException {
        if (parent != null && !LockType.canBeParentLock(parent.getExplicitLockType(transaction), lockType))
            throw new InvalidLockException("Invalid parent lock type");
        // only need to traverse the children of this transaction.
        // the the comment on the top
        for (Long childContextNum : transactionChildren.getOrDefault(transaction.getTransNum(), new ArrayList<>())) {
            LockContext childContext = childContext(childContextNum);
            LockType childLockType = childContext.getExplicitLockType(transaction);
            if (!LockType.canBeParentLock(lockType, childLockType))
                throw new InvalidLockException("Invalid parent lock type");
        }
    }

    private void checkReadonly() 
    throws UnsupportedOperationException {
        if (readonly)
            throw new UnsupportedOperationException("This context is read only");
    }

    // this block needs to be atomic since it reads the value and then increase it
    // no need to put it in a synchronized block since current thread will only access
    // the key-value of the transaction that occupies current thread
    // no other thread would access it, and any access on current thread is
    // sequential, so there is no race between this particular resource
    // and the map itself is concurrent hash map so read write to different keys won't be a problem
    private void parentAddChildContext(TransactionContext transaction, long context) {
        if (parent != null) {
            long transactionNum = transaction.getTransNum();
            int numLocks = parent.numChildLocks.getOrDefault(transactionNum, 0);
            parent.numChildLocks.put(transactionNum, numLocks + 1);
            List<Long> parentChildContext = parent.transactionChildren.getOrDefault(transactionNum, new ArrayList<>());
            assert !parentChildContext.contains(context);
            parentChildContext.add(context); // Not a referrence, need to put it back
            parent.transactionChildren.put(transactionNum, parentChildContext);
        }
    }

    private void parentRemoveChildContext(TransactionContext transaction) {
        if (parent != null) {
            long transactionNum = transaction.getTransNum();
            long context = getContextNum();
            int numLocks = parent.numChildLocks.getOrDefault(transactionNum, 1);
            parent.numChildLocks.put(transactionNum, numLocks - 1);
            List<Long> parentChildContext = parent.transactionChildren.get(transactionNum); 
            assert parentChildContext.contains(context);
            parentChildContext.remove(context); 
            parent.transactionChildren.put(transactionNum, parentChildContext);
        }
    }

    private long getContextNum() {
        return name.getCurrentName().getSecond();
    }


    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

