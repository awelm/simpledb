package simpledb;

import javafx.util.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.LinkedList;

public class DependencyGraph {
    private HashMap<TransactionId, HashSet<Pair<PageId, TransactionId>>> blockingMe; // waiting tids are the keys
    private HashMap<TransactionId, HashSet<Pair<PageId, TransactionId>>> waitingOnMe;

    public DependencyGraph() {
        blockingMe = new  HashMap<TransactionId, HashSet<Pair<PageId, TransactionId>>>();
        waitingOnMe = new HashMap<TransactionId, HashSet<Pair<PageId, TransactionId>>>();
    }

    public synchronized void addDependency(TransactionId waiter, TransactionId runner, PageId pid) throws TransactionAbortedException {
        if(waiter.equals(runner)) // block self-looping dependencies
            return;

        // add runner to graph in order to properly keep track of graph size
        if(!blockingMe.containsKey(runner))
            blockingMe.put(runner, new HashSet<Pair<PageId, TransactionId>>());

        // update incoming and outgoing edges in dependency graph
        Pair<PageId, TransactionId> newBlockingMeValue = new Pair<PageId, TransactionId>(pid, runner);
        if(!blockingMe.containsKey(waiter))
            blockingMe.put(waiter, new HashSet<Pair<PageId, TransactionId>>());
        blockingMe.get(waiter).add(newBlockingMeValue);

        if(!waitingOnMe.containsKey(runner))
            waitingOnMe.put(runner, new HashSet<Pair<PageId, TransactionId>>());
        Pair<PageId, TransactionId> newWaitingOnMeValue = new Pair<PageId, TransactionId>(pid, waiter);
        waitingOnMe.get(runner).add(newWaitingOnMeValue);

        if(detectDeadlock()) {
            _removeDependency(waiter, runner, pid);
            throw new TransactionAbortedException();
        }
    }

    private  void _removeDependency(TransactionId finishedRunning, TransactionId waiting, PageId pid) {
        if(!blockingMe.containsKey(waiting))
            return;
        HashSet<Pair<PageId, TransactionId>> waitingOn = blockingMe.get(waiting);
        Pair<PageId, TransactionId> completed = new Pair<PageId, TransactionId>(pid, finishedRunning);
        waitingOn.remove(completed);
        Pair<PageId, TransactionId> waitingOnMeValueToDelete = new Pair<PageId, TransactionId>(pid, waiting);
        waitingOnMe.get(finishedRunning).remove(waitingOnMeValueToDelete);
    }

    public synchronized void removeDependencies(TransactionId finishedRunning, PageId pid) {
        if(!waitingOnMe.containsKey(finishedRunning))
            return;
        HashSet<Pair<PageId, TransactionId>> blockedByMe = (HashSet<Pair<PageId, TransactionId>>) waitingOnMe.get(finishedRunning).clone();
        for(Pair<PageId,TransactionId> blocked : blockedByMe) {
            if(blocked.getKey().equals(pid))
                _removeDependency(finishedRunning, blocked.getValue(), pid);
        }
    }

    // use topological sort to detect cycles in this disconnected DAG with parallel edges
    private boolean detectDeadlock() {

        // calculate in-degrees
        HashMap<TransactionId, Integer> indegrees = new HashMap<TransactionId, Integer>();
        for(Map.Entry<TransactionId, HashSet<Pair<PageId, TransactionId>>> entry : blockingMe.entrySet()) {
            TransactionId source = entry.getKey();
            if(!indegrees.containsKey(source))
                indegrees.put(source, 0);

            HashSet<Pair<PageId, TransactionId>> destinations = entry.getValue();
            for(Pair<PageId, TransactionId> p : destinations) {
                TransactionId dest = p.getValue();
                if(!indegrees.containsKey(dest))
                    indegrees.put(dest, 0);
                indegrees.put(dest, indegrees.get(dest)+1);
            }
        }

        // find sources
        Queue<TransactionId> sources = new LinkedList<TransactionId>();
        for(Map.Entry<TransactionId, Integer> entry : indegrees.entrySet()) {
            TransactionId currSource = entry.getKey();
            int indegree = entry.getValue();
            if(indegree == 0) {
                int outdegree = blockingMe.get(currSource).size();
                if(outdegree == 0)
                    blockingMe.remove(currSource); // remove nodes with zero indegree and outdegree
                else
                    sources.add(entry.getKey());
            }
        }

        int visitedCount = 0;
        // decrement in-degrees while removing sources
        while(sources.size() > 0) {
            TransactionId current = sources.poll();
            visitedCount++;
            HashSet<Pair<PageId, TransactionId>> children = blockingMe.get(current);
            for(Pair<PageId, TransactionId> p : children) {
                TransactionId childId = p.getValue();
                indegrees.put(childId, indegrees.get(childId)-1);
                if(indegrees.get(childId) == 0)
                    sources.add(childId);
            }
        }

        return visitedCount != blockingMe.size();
    }
}
