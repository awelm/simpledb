# simpledb
A simple database built from scratch that has some of the basic RDBMS features (SQL query parser, transactions, query optimizer).
This database was inspired from MIT's database systems course which has all their students implement a basic database from scratch.
After completing the course, I have been tweaking things and adding my own features/customizations.

# Database Design
The simpledb design is surprisingly more complicated/involved than I expected. Here is an overview of the design:

### Data Storage and Access Methods
Database rows are referred to as *tuples* in simpledb. Each tuple has a set of *fields* which represent the column values
for the given row. Currently the only 2 types of supported fields are strings and integers. All tuples in the same table have the same schema, and the current implementation of simpledb requires that all tuples with the same schema take up the same number of bytes regardless of the field values in the tuple. Tuples are stored in *pages*, which are stored on disk.

There can be different types of pages depending on the desired storage method (e.g. index pages, heap pages, etc) but simpledb currently only supports heap pages. Pages belonging to the same table are stored together under the `DbFile` interface, which provides an interface to read/write pages and tuples to disk. Each database table is stored as a DbFile instance. Using the `Catalog` singleton object, you can add new tables to the database, view the schema for a table, view primary keys, etc.. The `BufferPool` singleton object is how DbFile objects actually access and modify their pages. This allows BufferPool to cache frequently used pages in memory so that page access doesn't always have to go to disk. Once Bufferpool cache gets full, it will need to evict pages using some eviction algorithm. Currently the Bufferpool evicts pages using the *no-steal* algorithm in order ensure transaction isolation (see more on this in the Transactions section).


### Operators
The SQL query parser will take a SQL query and convert it into a *logical plan*. This logical plan represents the SQL query in a tree of relational algebra operators (project, select, join, etc.). The query optimizer will then take this logical plan and then convert it into a physical plan composed of physical operators by using cost-based optimization and equivalence rules.

The physical operators represent the actual execution of the query. Here is a list of the physical operators currently supported by simpledb: sequential table scans, insert, delete, order by, filter, aggregations, hash joins, and NL joins. Each operator in simpledb implements the `DbIterator` interface, which allows parent nodes of a given operator to extract tuples using *hasNext()* and *next()*. These tuples flow all the way from the leaves of tree to the root node of the tree while undergoing different transformations depending on the operators they flow through and then are eventually displayed to the user as query results. The leaf nodes of the tree are always going to be operators that read data from the buffer pool.


### Query Optimization
At a high level, the query optimizer takes a logical plan as input and attempts to convert it into the cheapest possible physical plan. Simpledb uses Selinger Optimization in order to determine the optimal join order in a query. In order to do that, we need table statistics data. `TableStats` creates histograms for each column in a given table and these statistics are used to estimate selectivity, scan costs, and cardinality. The Selinger optimizer will take these estimates and then provide us with the cheapest estimated ordering of joins. That being said, the join-order optimization problem has been proved to be an NP-complete problem. The Selinger Optimizer uses dynamic programming to try to make the exponential time more tractable, but at the end of the day there isn't much we can do about this.


### Transactions
Transactions are how simpledb ensures that ACID properties are satisfied. It should not be apparent to an outside observer that all the operations of a single transaction were not completed as part of one single, indivisible action. Transactions can be run in parallel, so some form of locking is necessary in order to avoid 2 transactions writing to the same locations in parallel. Simpledb use the strict 2PL concurrency control method and locks data at the page-level. `LockManager` provides support for both shared locks and exclusive locks in order to allow multiple readers to access the same data in parallel. Locks are grabbed when a transaction calls `BufferPool.getPage` and this function will block until the lock is acquired. Blocking in this fashion runs the risk of deadlock, which is why simpledb also implements a `DependencyGraph` which detects deadlocks using topological sort. Deadlock detection is also done in getPage() and the calling transaction will be aborted if it triggers a deadlock. All of the locks held by a transaction are released when a transaction is complete.

In order to implement isolation, we use the no-steal eviction algorithm in the buffer pool, which ensures that dirty pages will not be evicted from the buffer pool. When a transaction decides to commit, we always flush its dirty pages to disk in order to ensure the transaction's durability. If a transaction decides to abort, we will evict its dirty pages from the buffer pool. If the database crashes mid-transaction, all the dirty pages in memory will be lost but all the previous committed transactions will have been written to disk. When the database comes back online, all the previously interrupted transactions will have no remaining presence but all data modified by the committed transactions will be present since these changes were flushed to disk. This provides us with the atomicity guarantee that we want.


# Areas to Improve
* garbage collect heap file pages to prevent fragmentation
* remove the fixed tuple size requirement
* add support for indexes, clustered indexes
* sort-merge join
* add more query optimization features


# How to Run
Run the following to start up a simpledb REPL that will query an example database I have already set up:

```
ant
java -jar dist/simpledb.jar parser nsf.schema
```
Then enter a query into the REPL. For example try:

```
SELECT g.title
FROM grants g
WHERE g.title LIKE 'Monkey';
```

For something more intensive try this:

```
SELECT r2.name, count(g.id)
FROM grants g, researchers r, researchers r2, grant_researchers gr,
grant_researchers gr2
WHERE r.name = 'Samuel Madden'
AND gr.researcherid = r.id
AND gr.grantid = g.id
AND gr2.researcherid = r2.id
AND gr.grantid = gr2.grantid
GROUP BY r2.name
ORDER BY r2.name;
 ```
