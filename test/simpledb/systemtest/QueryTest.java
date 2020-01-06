package simpledb.systemtest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;

import org.junit.Assert;
import org.junit.Test;

import simpledb.BufferPool;
import simpledb.Database;
import simpledb.DbException;
import simpledb.HeapFile;
import simpledb.HeapFileEncoder;
import simpledb.JoinOptimizer;
import simpledb.LogicalJoinNode;
import simpledb.Parser;
import simpledb.Predicate;
import simpledb.TableStats;
import simpledb.Transaction;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;
import simpledb.Utility;

public class QueryTest {
	
	/**
	 * Given a matrix of tuples from SystemTestUtil.createRandomHeapFile, create an identical HeapFile table
	 * @param tuples Tuples to create a HeapFile from
	 * @param columns Each entry in tuples[] must have "columns == tuples.get(i).size()"
	 * @param colPrefix String to prefix to the column names (the columns are named after their column number by default)
	 * @return a new HeapFile containing the specified tuples
	 * @throws IOException if a temporary file can't be created to hand to HeapFile to open and read its data
	 */
	public static HeapFile createDuplicateHeapFile(ArrayList<ArrayList<Integer>> tuples, int columns, String colPrefix) throws IOException {
        File temp = File.createTempFile("table", ".dat");
        temp.deleteOnExit();
        HeapFileEncoder.convert(tuples, temp, BufferPool.PAGE_SIZE, columns);
        return Utility.openHeapFile(columns, colPrefix, temp);
	}
	
	@Test(timeout=20000) public void queryTest() throws IOException, DbException, TransactionAbortedException {
		// This test is intended to approximate the join described in the
		// "Query Planning" section of 2009 Quiz 1,
		// though with some minor variation due to limitations in simpledb
		// and to only test your integer-heuristic code rather than
		// string-heuristic code.		
		final int IO_COST = 101;
		
		HashMap<String, TableStats> stats = new HashMap<String, TableStats>();
		
		// Create all of the tables, and add them to the catalog
		ArrayList<ArrayList<Integer>> empTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile emp = SystemTestUtil.createRandomHeapFile(6, 100000, null, empTuples, "c");	
		Database.getCatalog().addTable(emp, "emp");
		
		ArrayList<ArrayList<Integer>> deptTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile dept = SystemTestUtil.createRandomHeapFile(3, 1000, null, deptTuples, "c");	
		Database.getCatalog().addTable(dept, "dept");
		
		ArrayList<ArrayList<Integer>> hobbyTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile hobby = SystemTestUtil.createRandomHeapFile(6, 1000, null, hobbyTuples, "c");
		Database.getCatalog().addTable(hobby, "hobby");
		
		ArrayList<ArrayList<Integer>> hobbiesTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile hobbies = SystemTestUtil.createRandomHeapFile(2, 200000, null, hobbiesTuples, "c");
		Database.getCatalog().addTable(hobbies, "hobbies");
		
		// Get TableStats objects for each of the tables that we just generated.
		stats.put("emp", new TableStats(Database.getCatalog().getTableId("emp"), IO_COST));
		stats.put("dept", new TableStats(Database.getCatalog().getTableId("dept"), IO_COST));
		stats.put("hobby", new TableStats(Database.getCatalog().getTableId("hobby"), IO_COST));
		stats.put("hobbies", new TableStats(Database.getCatalog().getTableId("hobbies"), IO_COST));

		Parser.setStatsMap(stats);
		
		Transaction t = new Transaction();
		t.start();
		Parser.setTransaction(t);
		
		// Each of these should return around 20,000
		// This Parser implementation currently just dumps to stdout, so checking that isn't terribly clean.
		// So, don't bother for now; future TODO.
		// Regardless, each of the following should be optimized to run quickly,
		// even though the worst case takes a very long time.
		Parser.processNextStatement("SELECT * FROM emp,dept,hobbies,hobby WHERE emp.c1 = dept.c0 AND hobbies.c0 = emp.c2 AND hobbies.c1 = hobby.c0 AND emp.c3 < 1000;");
	}
	
	/**
	 * Build a large series of tables; then run the command-line query code and execute a query.
	 * The number of tables is large enough that the query will only succeed within the
	 * specified time if a join method faster than nested-loops join is available.
	 * The tables are also too big for a query to be successful if its query plan isn't reasonably efficient,
	 * and there are too many tables for a brute-force search of all possible query plans.
	 */
	// Not required for Lab 4
	/*@Test(timeout=60000) public void hashJoinTest() throws IOException, DbException, TransactionAbortedException {
		final int IO_COST = 103;
		
		HashMap<String, TableStats> stats = new HashMap<String,TableStats>();
				
		ArrayList<ArrayList<Integer>> smallHeapFileTuples = new ArrayList<ArrayList<Integer>>();
		HeapFile smallHeapFileA = SystemTestUtil.createRandomHeapFile(2, 100, Integer.MAX_VALUE, null, smallHeapFileTuples, "c");		
		HeapFile smallHeapFileB = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileC = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileD = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileE = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileF = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");		
		HeapFile smallHeapFileG = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileH = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileI = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileJ = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileK = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileL = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileM = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		HeapFile smallHeapFileN = createDuplicateHeapFile(smallHeapFileTuples, 2, "c");
		
		ArrayList<ArrayList<Integer>> bigHeapFileTuples = new ArrayList<ArrayList<Integer>>();
		for (int i = 0; i < 1000; i++) {
			bigHeapFileTuples.add( smallHeapFileTuples.get( i%100 ) );
		}
		HeapFile bigHeapFile = createDuplicateHeapFile(bigHeapFileTuples, 2, "c");
		Database.getCatalog().addTable(bigHeapFile, "bigTable");

		// We want a bunch of these guys
		Database.getCatalog().addTable(smallHeapFileA, "a");
		Database.getCatalog().addTable(smallHeapFileB, "b");
		Database.getCatalog().addTable(smallHeapFileC, "c");
		Database.getCatalog().addTable(smallHeapFileD, "d");
		Database.getCatalog().addTable(smallHeapFileE, "e");
		Database.getCatalog().addTable(smallHeapFileF, "f");
		Database.getCatalog().addTable(smallHeapFileG, "g");
		Database.getCatalog().addTable(smallHeapFileH, "h");
		Database.getCatalog().addTable(smallHeapFileI, "i");
		Database.getCatalog().addTable(smallHeapFileJ, "j");
		Database.getCatalog().addTable(smallHeapFileK, "k");
		Database.getCatalog().addTable(smallHeapFileL, "l");
		Database.getCatalog().addTable(smallHeapFileM, "m");
		Database.getCatalog().addTable(smallHeapFileN, "n");
		
		stats.put("bigTable", new TableStats(bigHeapFile.getId(), IO_COST));
		stats.put("a", new TableStats(smallHeapFileA.getId(), IO_COST));
		stats.put("b", new TableStats(smallHeapFileB.getId(), IO_COST));
		stats.put("c", new TableStats(smallHeapFileC.getId(), IO_COST));
		stats.put("d", new TableStats(smallHeapFileD.getId(), IO_COST));
		stats.put("e", new TableStats(smallHeapFileE.getId(), IO_COST));
		stats.put("f", new TableStats(smallHeapFileF.getId(), IO_COST));
		stats.put("g", new TableStats(smallHeapFileG.getId(), IO_COST));
		stats.put("h", new TableStats(smallHeapFileG.getId(), IO_COST));
		stats.put("i", new TableStats(smallHeapFileG.getId(), IO_COST));
		stats.put("j", new TableStats(smallHeapFileG.getId(), IO_COST));
		stats.put("k", new TableStats(smallHeapFileG.getId(), IO_COST));
		stats.put("l", new TableStats(smallHeapFileG.getId(), IO_COST));
		stats.put("m", new TableStats(smallHeapFileG.getId(), IO_COST));
		stats.put("n", new TableStats(smallHeapFileG.getId(), IO_COST));

		Parser.setStatsMap(stats);
		
		Transaction t = new Transaction();
		t.start();
		Parser.setTransaction(t);
		
		// Each of these should return around 20,000
		// This Parser implementation currently just dumps to stdout, so checking that isn't terribly clean.
		// So, don't bother for now; future TODO.
		// Regardless, each of the following should be optimized to run quickly,
		// even though the worst case takes a very long time.
		Parser.processNextStatement("SELECT COUNT(a.c0) FROM bigTable, a, b, c, d, e, f, g, h, i, j, k, l, m, n WHERE bigTable.c0 = n.c0 AND a.c1 = b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1 AND d.c0 = e.c0 AND e.c1 = f.c1 AND f.c0 = g.c0 AND g.c1 = h.c1 AND h.c0 = i.c0 AND i.c1 = j.c1 AND j.c0 = k.c0 AND k.c1 = l.c1 AND l.c0 = m.c0 AND m.c1 = n.c1;");
		Parser.processNextStatement("SELECT COUNT(a.c0) FROM bigTable, a, b, c, d, e, f, g, h, i, j, k, l, m, n WHERE a.c1 = b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1 AND d.c0 = e.c0 AND e.c1 = f.c1 AND f.c0 = g.c0 AND g.c1 = h.c1 AND h.c0 = i.c0 AND i.c1 = j.c1 AND j.c0 = k.c0 AND k.c1 = l.c1 AND l.c0 = m.c0 AND m.c1 = n.c1 AND bigTable.c0 = n.c0;");
		Parser.processNextStatement("SELECT COUNT(a.c0) FROM bigTable, a, b, c, d, e, f, g, h, i, j, k, l, m, n WHERE k.c1 = l.c1 AND a.c1 = b.c1 AND f.c0 = g.c0 AND bigTable.c0 = n.c0 AND d.c0 = e.c0 AND c.c1 = d.c1 AND e.c1 = f.c1 AND i.c1 = j.c1 AND b.c0 = c.c0 AND g.c1 = h.c1 AND h.c0 = i.c0 AND j.c0 = k.c0 AND m.c1 = n.c1 AND l.c0 = m.c0;");
	}*/
}
