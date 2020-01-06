package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


class Lock {
    TransactionId tid;
    PageId pid;
    boolean exclusive;

    public Lock(TransactionId tid, PageId pid, boolean exclusive) {
        this.tid = tid;
        this.pid = pid;
        this.exclusive = exclusive;
    }

    public boolean equals(Object o) {
        if(!(o instanceof Lock))
            return false;
        Lock other = (Lock) o;
        return this.tid.equals(other.tid) && this.pid.equals(other.pid) && this.exclusive == other.exclusive;
    }

    @Override
    public int hashCode() {
        return (tid != null ? tid.hashCode() : 0) + pid.hashCode() + (exclusive ? 1 : 0);
    }
}



public class LockManager {
    private HashMap<PageId, ArrayList<Lock>> pageIdToLocks;
    private HashMap<TransactionId, HashSet<PageId>> txIdToPageIds;
    private DependencyGraph dependencyGraph;

    public LockManager() {
        pageIdToLocks = new HashMap<PageId, ArrayList<Lock>>();
        txIdToPageIds = new HashMap<TransactionId, HashSet<PageId>>();
        dependencyGraph = new DependencyGraph();
    }

    public synchronized boolean lockPage(TransactionId tid, PageId pid, boolean exclusiveLock) throws TransactionAbortedException {
        // return early if requestor is asking for a lock they already have
        Permissions existingPermissions = getLockHeldType(tid, pid);
        if(existingPermissions != null)
            if(existingPermissions.isReadWrite() || (!exclusiveLock && existingPermissions.isReadOnly()))
                return true;

        boolean canAquire = false;
        ArrayList<Lock> otherLocks = null;
        if(pageIdToLocks.containsKey(pid)) {
            otherLocks = pageIdToLocks.get(pid);
            if(otherLocks.isEmpty())
                canAquire = true;
            else {
                if(upgradeLock(tid, pid))
                    return true;
                boolean otherExclusiveLock = false;
                for(Lock l : otherLocks)
                    if(l.exclusive)
                        otherExclusiveLock = true;
                if(!exclusiveLock && !otherExclusiveLock)
                    canAquire = true;
            }
        } else {
            canAquire = true;
        }

        if(!canAquire) {
            addDependencies(tid, pid, exclusiveLock, otherLocks);
            return false;
        }

        Lock newLock = new Lock(tid, pid, exclusiveLock);
        if(!pageIdToLocks.containsKey(pid))
            pageIdToLocks.put(pid, new ArrayList<Lock>());
        pageIdToLocks.get(pid).add(newLock);

        if(!txIdToPageIds.containsKey(tid))
            txIdToPageIds.put(tid, new HashSet<PageId>());
        txIdToPageIds.get(tid).add(pid);

        return true;
    }

    private Permissions getLockHeldType(TransactionId tid, PageId pid) {
       if(pageIdToLocks.containsKey(pid)) {
            ArrayList<Lock> locks = pageIdToLocks.get(pid);
            for(Lock l : locks)
                if(l.tid.equals(tid))
                    return l.exclusive ? Permissions.READ_WRITE : Permissions.READ_ONLY;
       }
       return null;
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        if(!txIdToPageIds.containsKey(tid))
            return false;
        return txIdToPageIds.get(tid).contains(pid);
    }

    public synchronized void unlockPage(TransactionId tid, PageId pid) {
        if(!pageIdToLocks.containsKey(pid))
            return;

        Iterator<Lock> pageLockIterator = pageIdToLocks.get(pid).iterator();
        while(pageLockIterator.hasNext()) {
            Lock l = pageLockIterator.next();
            if(l.tid.equals(tid))
                pageLockIterator.remove();
        }

        dependencyGraph.removeDependencies(tid, pid);

        if(txIdToPageIds.containsKey(tid))
            txIdToPageIds.get(tid).remove(pid);
    }

    public void unlockAllPages(TransactionId tid) {
        HashSet<PageId> pageIds;

        synchronized (this) {
            if(!txIdToPageIds.containsKey(tid))
                return;

            pageIds = txIdToPageIds.get(tid);
            txIdToPageIds.remove(tid);
        }

        for(PageId pid : pageIds)
            unlockPage(tid, pid);
    }

    public synchronized Set<PageId> getPagesLockedByTx(TransactionId tid) {
        return txIdToPageIds.get(tid);
    }

    private boolean upgradeLock(TransactionId tid, PageId pid) {
        if(pageIdToLocks.containsKey(pid)) {
            ArrayList<Lock> pageLocks = pageIdToLocks.get(pid);
            if(pageLocks.size() == 1) {
               Lock onlyLock = pageLocks.iterator().next();
               if(onlyLock.tid.equals(tid)) {
                   onlyLock.exclusive = true;
                   return true;
               }
            }
        }
        return false;
    }

    private void addDependencies(TransactionId tid, PageId pid, boolean exclusiveLock, ArrayList<Lock> otherLocks) throws TransactionAbortedException {
        // if requested lock is exclusive, we must wait for all other locks to be released
        if(exclusiveLock) {
            for(Lock l : otherLocks)
                dependencyGraph.addDependency(tid, l.tid, pid);
        } else {
            for(Lock l : otherLocks)
                if(l.exclusive)
                    dependencyGraph.addDependency(tid, l.tid, pid);
        }
    }
}
