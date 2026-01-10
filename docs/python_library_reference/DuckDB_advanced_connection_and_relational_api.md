# DuckDB Python Client – Advanced Connection and Relational API Usage (Continued)

## 1.5 Connection API Advanced Operations

Advanced operations of DuckDB’s Python **connection API** enable robust query parameterization, custom function definitions, multi-database management, and more. These features help expert users write secure, high-performance code and integrate DuckDB with various data sources.

### Parameterized Queries and Prepared Statements

DuckDB’s Python API supports **parameterized SQL queries** following the Python DB-API convention[\[1\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=The%20API%20follows%20DB). Instead of embedding variables in SQL strings (which can risk SQL injection), use placeholders in the query and supply values separately. DuckDB accepts:

- `?` **placeholders** for positional parameters[\[1\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=The%20API%20follows%20DB).
- `$1, $2, …` **numbered placeholders**, and
- `$name` **named placeholders**[\[2\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Use%20%3F%20placeholders%20in%20your,68).

You pass the actual parameters as a second argument to `execute()` – either a list/tuple for positional or a dict for named parameters[\[1\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=The%20API%20follows%20DB). DuckDB prepares the query, binds the parameters, and executes efficiently. For example:

    conn.execute("CREATE TABLE items(item VARCHAR, value INT, qty INT)")

    # Single insert using positional parameters (with ? placeholders)
    conn.execute("INSERT INTO items VALUES (?, ?, ?)", ["laptop", 2000, 1])

    # Batch insert multiple rows using executemany (reuses prepared statement)
    conn.executemany("INSERT INTO items VALUES (?, ?, ?)", [
        ["chainsaw", 500, 10],
        ["iphone",   300, 2]
    ])

    # Query with a parameter
    min_value = 400
    conn.execute("SELECT item FROM items WHERE value > ?", [min_value])
    print(conn.fetchall())  # e.g., [('laptop',), ('chainsaw',)]

In the above snippet, `?` placeholders are replaced by the provided values in order[\[3\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=conn.execute%28,e.g.%2C%20%5B%28%27laptop%27%2C%29%2C%20%28%27chainsaw)[\[4\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=In%20the%20above%3A%20,it%20for%20each%20parameter%20list). Using `executemany()` for batch inserts prepares the statement once and executes it for each parameter list[\[4\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=In%20the%20above%3A%20,it%20for%20each%20parameter%20list), which is more efficient than calling `execute()` in a loop. DuckDB also supports **named parameters** using the `$name` syntax: supply a dictionary of names to values. For example:

    result = conn.execute(
        "SELECT $greet || ', ' || $noun",
        {"greet": "Hello", "noun": "DuckDB"}
    ).fetchone()
    print(result)  # Outputs: ('Hello, DuckDB',)

Here `$greet` and `$noun` in the SQL are replaced by the corresponding values from the dict[\[5\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Here%20%24greet%20and%20%24noun%20in,will%20use%20the%20same%20value). You can reuse the same named parameter multiple times in the query – each occurrence of `$name` uses the provided value[\[5\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Here%20%24greet%20and%20%24noun%20in,will%20use%20the%20same%20value). DuckDB also allows **numbered parameters** `$1, $2, ...` as an alternative, which correspond to positions in the parameter list[\[6\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=occurrence%20of%20%24name%20will%20use,the%20same%20value).

After executing a parameterized query, you can retrieve results using standard DB-API cursor methods, since the connection object acts as its own cursor. Key methods include `fetchone()` (next row or `None`), `fetchall()` (all rows as a list of tuples), and `fetchmany(n)` (next *n* rows). You may call these on the connection directly or on the result of `execute()`[\[7\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=DB,74). The `description` property provides column metadata (names, types) for the last query[\[7\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=DB,74).

**Security Note:** Always use placeholders rather than Python string formatting to inject variables into SQL[\[8\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=property%20gives%20column%20names%20of,74). This ensures queries are safely parameterized and not vulnerable to injection attacks. DuckDB will handle quoting and typing of parameters internally.

**Performance Note:** DuckDB caches prepared statements under the hood. If you execute the same parameterized SQL string repeatedly (with different values), it will likely reuse the compiled query plan, improving performance[\[9\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Note%20on%20performance%3A%20DuckDB%20can,77). For bulk inserts, using `executemany()` is fine for a modest number of rows. However, for very large inserts (e.g. millions of rows), it’s more efficient to use other techniques: for instance, create a Pandas DataFrame and do `CREATE TABLE ... AS SELECT * FROM df` (utilizing DuckDB’s DataFrame import, discussed later) or use the **Appender** or **COPY** interface for bulk loading[\[9\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Note%20on%20performance%3A%20DuckDB%20can,77). These approaches avoid thousands of Python function calls and leverage DuckDB’s optimized loaders.

### Defining Python UDFs (User-Defined Functions)

DuckDB allows registering **Python functions as UDFs** that can be invoked directly in SQL queries. This powerful feature lets you extend DuckDB’s SQL with arbitrary Python logic while still executing the rest of the query in DuckDB’s engine[\[10\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=DuckDB%20allows%20you%20to%20define,leveraging%20DuckDB%20for%20data%20access). UDFs can operate in two modes:

- **Scalar UDFs (per-row)** – the Python function is called for each row (each value or set of values).
- **Vectorized UDFs (per-column or chunk)** – DuckDB passes entire columns of data as Apache Arrow arrays to your function, allowing vectorized operations (using NumPy, pandas, etc.) for performance[\[11\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Vectorized%20,For%20example)[\[12\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Here%2C%20DuckDB%20calls%20vector_add%20only,be%20directly%20used%20or%20converted).

You register a UDF with `conn.create_function(name, function, parameters, return_type, **options)`[\[13\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Use%20conn,for%20large%20data%20due%20to). Important parameters include:

- `name`: The string name to use for the function in SQL.
- `function`: The actual Python callable (a function or lambda).
- `parameters`: A list of DuckDB data types for the function arguments. You can use types from `duckdb.sqltypes` (e.g. `INTEGER`, `VARCHAR`) or type strings like `'INT'`. If you omit this or pass `None`, DuckDB can often **infer types from Python type hints** on your function[\[14\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=the%20actual%20Python%20callable%20%28e,for%20large%20data%20due%20to).
- `return_type`: DuckDB data type of the return value, similarly specified.
- `**options`: Additional options controlling UDF behavior. Key options include:
- `type`: `'native'` (default) for scalar UDF or `'arrow'` for vectorized (Arrow) UDF[\[15\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=%28e,83%5D.%20If%20you%20want). In native mode, DuckDB calls your function for each value; in arrow mode, it calls it once per batch of column data (much faster for large datasets)[\[11\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Vectorized%20,For%20example)[\[12\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Here%2C%20DuckDB%20calls%20vector_add%20only,be%20directly%20used%20or%20converted).
- `null_handling`: `'NULL'` (default) or `'special'`. By default, if any input to your UDF is NULL, DuckDB will skip calling your function and directly return NULL (treating your UDF as strict)[\[16\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=function%20for%20each%20value,you%20can%20decide%20what%20to). If you set `null_handling='special'`, DuckDB will pass Python `None` for SQL NULLs, allowing your function to handle them explicitly (e.g., to implement custom null behavior)[\[17\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=conn.create_function%28,instead%20of%20stopping%20the%20query).
- `exception_handling`: `'throw'` (default) or `'return_null'`. By default, any exception inside your UDF will propagate and abort the query[\[17\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=conn.create_function%28,instead%20of%20stopping%20the%20query). If `exception_handling='return_null'`, then if your function raises an error for a given row/chunk, DuckDB will catch it and substitute NULL for that result, allowing the query to continue[\[17\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=conn.create_function%28,instead%20of%20stopping%20the%20query). This is useful for making robust UDFs that produce NULL on bad data instead of failing the entire query.
- `side_effects`: `False` (default) or `True`. Defaults assume your UDF is pure (deterministic with no side effects), which allows DuckDB to safely reuse or parallelize calls. If your function has side effects or depends on external state (e.g., uses random numbers or modifies global variables), set `side_effects=True` so DuckDB knows not to optimize under the assumption of determinism[\[18\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Use%20conn,for%20large%20data%20due%20to)[\[19\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=annotations%2C%20you%20can%20often%20pass,set%20null_handling%3D%27special%27%20%E2%80%93%20then%20your).

**Example – Scalar UDF:** Define a simple scalar function to add 10 to a number:

    def add_ten(x: int) -> int:
        return x + 10

    conn.create_function("add_ten", add_ten)
    print(conn.sql("SELECT add_ten(5)").fetchone())  # Outputs: (15,)

We did not explicitly specify parameter or return types here, relying on the Python type hints (`int -> int`) to let DuckDB infer them[\[20\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=def%20add_ten%28x%3A%20int%29%20,return%20x%20%2B%2010). The UDF `add_ten` can now be used in any SQL query on this connection. For instance, `SELECT add_ten(column) FROM table` would apply the Python function to each value of `column`.

**Example – Handling NULLs and Exceptions:** The following UDF divides two numbers but returns NULL (None) if the divisor is zero, demonstrating custom null handling:

    def safe_divide(a: float, b: float) -> float:
        if b == 0:
            return None  # return NULL in DuckDB
        return a / b

    conn.create_function(
        "safe_divide", safe_divide, 
        [duckdb.sqltypes.DOUBLE, duckdb.sqltypes.DOUBLE], duckdb.sqltypes.DOUBLE,
        null_handling='special', exception_handling='return_null'
    )
    # Now SQL queries can use safe_divide(x,y); division by zero yields NULL instead of error.

Here we provided explicit parameter types (two DOUBLEs) and return type, and set `null_handling='special'` so that our function receives `None` when either input is SQL NULL[\[17\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=conn.create_function%28,instead%20of%20stopping%20the%20query). We also set `exception_handling='return_null'` so that if any unexpected error occurs inside `safe_divide`, DuckDB will return NULL for that row rather than halting execution[\[17\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=conn.create_function%28,instead%20of%20stopping%20the%20query). For example, `SELECT safe_divide(10, 0)` would return SQL NULL (as handled by our function), and any internal exception would also result in NULL instead of an error in the query.

**Example – Vectorized (Arrow) UDF:** Below, we define a UDF that adds two arrays element-wise using NumPy. By specifying `type='arrow'`, DuckDB will pass entire columns as Arrow arrays to the function in one go, enabling vectorized computation[\[11\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Vectorized%20,For%20example)[\[12\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Here%2C%20DuckDB%20calls%20vector_add%20only,be%20directly%20used%20or%20converted):

    import numpy as np

    def vector_add(x_arrow, y_arrow):
        # x_arrow and y_arrow are pyarrow.Array objects
        x = np.array(x_arrow)  # zero-copy convert Arrow array to numpy
        y = np.array(y_arrow)
        return x + y  # NumPy will add elementwise

    conn.create_function(
        "vector_add", vector_add, 
        [duckdb.sqltypes.BIGINT, duckdb.sqltypes.BIGINT], duckdb.sqltypes.BIGINT,
        type='arrow'
    )

    # Using the UDF in SQL (e.g., add pairs of numbers):
    result = conn.sql("""
        SELECT vector_add(i, j) AS sum
        FROM (VALUES (1,2), (3,4)) AS t(i,j)
    """).fetchall()
    print(result)  # Outputs: [(3,), (7,)]

In this example, DuckDB calls `vector_add` only **once per data chunk** (not once per row). The function internally leverages NumPy to perform fast vectorized addition on the arrays. The Arrow-to-NumPy conversion is zero-copy (no data duplication) and highly efficient[\[21\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=import%20numpy%20as%20np%20def,elementwise%20addition)[\[12\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Here%2C%20DuckDB%20calls%20vector_add%20only,be%20directly%20used%20or%20converted). This means even very large columns can be processed quickly. Just ensure the necessary libraries (e.g., `pyarrow` for Arrow, `numpy` for NumPy) are installed and that the entire column fits in memory.

*Performance considerations:* Python UDFs execute within DuckDB’s process (under the hood, DuckDB runs them in the embedded Python interpreter). While they offer great flexibility, **Python UDFs will be slower than equivalent built-in SQL functions**, especially in scalar mode, due to Python’s overhead. Whenever possible, prefer DuckDB’s native SQL operations for large data transformations, and use UDFs for logic that cannot be expressed in SQL[\[22\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Resource%20management%3A%20Python%20UDFs%20execute,are%20invaluable%20for%20custom%20logic). Vectorized UDFs mitigate some overhead by processing batches of data at once, but still run Python code. Monitor UDF performance and consider alternatives (built-in functions or moving logic to where data is smaller) if performance is critical. You can remove a UDF with `conn.remove_function('function_name')` if needed[\[22\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Resource%20management%3A%20Python%20UDFs%20execute,are%20invaluable%20for%20custom%20logic).

DuckDB UDFs even support **closures/partial application**: you can pre-bind certain arguments of your Python function with `functools.partial` before registering it[\[23\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=unregister%20the%20function%20from%20the,connection%E2%80%99s%20catalog). This allows creation of specialized UDFs where some configuration is fixed at registration time (for example, a UDF that always multiplies by a fixed constant defined when registering).

### Using DuckDB Extensions

DuckDB’s functionality can be expanded via **extensions** – modular add-ons that provide new SQL functions, data types, or connectivity. Many extensions come built-in with the DuckDB Python package (or will auto-download when first used), covering areas like web access, JSON parsing, Excel files, Parquet/DeltaLake/Iceberg access, and more[\[24\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=DuckDB%E2%80%99s%20functionality%20can%20be%20extended,cover%20connectivity%20and%20file%20formats)[\[25\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=httpfs%3A%20This%20extension%20enables%20DuckDB,you%20want%20to%20be%20sure).

**Installing/Loading Extensions:** By default, DuckDB can auto-install and load known official extensions on first use (controlled by the settings `autoinstall_known_extensions` and `autoload_known_extensions`, which are true by default)[\[26\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Installing%2FLoading%20Extensions%3A%20DuckDB%20has%20an,you%20can%20also%20explicitly%20install%2Fload). For example, if you query an `s3://...` URL without having loaded the S3 support, DuckDB will automatically load the `httpfs` extension. You can also manage extensions manually:

- Use `conn.execute("INSTALL 'extension_name'")` to download an extension (if not already installed)[\[26\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Installing%2FLoading%20Extensions%3A%20DuckDB%20has%20an,you%20can%20also%20explicitly%20install%2Fload). This typically fetches a binary from DuckDB’s extension repository.
- Use `conn.execute("LOAD 'extension_name'")` to load the extension into the current connection (enabling its features)[\[27\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=conn.execute%28%22INSTALL%20%27json%27%3B%22%29%20%20%20,install%20if%20needed). If the extension was not installed, `LOAD` will try to install it first automatically.

For example, to ensure the JSON extension is loaded (for JSON file querying and JSON functions), you could do:

    conn.execute("INSTALL 'json';")
    conn.execute("LOAD 'json';")

In practice, you often don’t need to manually install core extensions like `httpfs` or `json` in Python—DuckDB will handle it when you use them[\[28\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=For%20Python%2C%20many%20extensions%20,loading%20is%20disabled). The above is useful if you want to guarantee an extension is available or if you’ve disabled auto-install for security.

**Common Extensions and Capabilities:**

- `httpfs` **(HTTP/Cloud access):** Enables DuckDB to read from web URLs (`http://`, `https://`) and cloud storage (`s3://`, `gs://` for Google Cloud, etc.) with proper authentication. It also provides faster encryption support via OpenSSL. In the Python client, `httpfs` is usually bundled and may load automatically when needed[\[25\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=httpfs%3A%20This%20extension%20enables%20DuckDB,you%20want%20to%20be%20sure). Once loaded, you can query remote files directly. *Example:* after loading `httpfs`, a query like `SELECT COUNT(*) FROM 'https://example.com/data.parquet'` works out-of-the-box (DuckDB will stream-download the Parquet file as needed). For AWS S3, you can use `s3://bucket/path` URLs; with `httpfs`, DuckDB will utilize AWS credentials if configured (see **Secrets** below for managing keys).

- `json`**:** Provides JSON data type support and the `read_json` function to query JSON files. This extension is often built-in and auto-loaded on first use[\[29\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=json%3A%20The%20JSON%20extension%20provides,functions%20for%20querying%20JSON%20structures). It allows querying JSON documents (including nested JSON) using functions like `json_extract`. For example, `SELECT * FROM read_json('data.json')` can parse JSON records from a file. If you encounter a missing function error for JSON functions, you might explicitly load this extension.

- `excel`**:** Allows reading Excel `.xlsx` files. After `INSTALL 'excel'; LOAD 'excel';`, you can use the `read_xlsx()` table function in SQL to read sheets[\[30\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=excel%3A%20The%20Excel%20extension%20allows,To%20use). For instance: `SELECT * FROM read_xlsx('report.xlsx', sheet='Sheet1')` will read the specified sheet. DuckDB treats each Excel sheet as a table. Writing to Excel is possible via `COPY ... TO 'file.xlsx' (FORMAT XLSX)`[\[31\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=conn.execute%28). (Note: older `.xls` files are not supported[\[32\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=auto,not%20present).)

- **Data Lake formats (**`delta`**,** `iceberg`**):** DuckDB can query Apache **Delta Lake** and **Iceberg** table formats via extensions. For Delta Lake, after `LOAD 'delta'`, you can query a Delta table directory using `SELECT * FROM delta.read_delta('path/to/delta_table')`[\[33\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=delta%3A%20The%20Delta%20Lake%20extension,Usage). This will read the Delta transaction log and present the latest snapshot of the table[\[34\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=SELECT%20). For Iceberg, after `LOAD 'iceberg'`, you need to configure a catalog (e.g., an AWS Glue or Hive Metastore) via `SET` commands, then you can query Iceberg tables by name[\[35\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=iceberg%3A%20The%20Iceberg%20extension%20allows,need%20to%20configure%20a%20catalog). These extensions allow DuckDB to act as a query engine on data lake files without needing Spark or Hive – DuckDB handles the metadata and reads the Parquet files behind the scenes.

- **External database attachments (**`postgres`**,** `mysql`**,** `sqlite`**):** DuckDB offers extensions to query external databases. For example, `LOAD 'postgres';` then using the `postgres_attach` function can link a remote Postgres database, making its tables queryable in DuckDB. Similarly for MySQL. The `sqlite` extension allows attaching a local SQLite database file and querying it as if it were part of DuckDB[\[36\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Other%20extensions%3A%20DuckDB%20has%20many,etc)[\[37\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Additionally%2C%20ATTACH%20supports%20attaching%20non,databases%20via%20extensions). (We’ll see an example of using `ATTACH ... (TYPE sqlite)` below.)

- **Other extensions:** There are many others (full text search `fts`, geospatial functions, MotherDuck cloud integration, etc.)[\[36\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Other%20extensions%3A%20DuckDB%20has%20many,etc). Each extension might have specific usage. Official extensions are cryptographically signed by DuckDB; by default, DuckDB will only auto-install signed extensions. Community extensions may require enabling `allow_unsigned_extensions` in configuration[\[38\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Keep%20in%20mind%20that%20community%2Fthird,install%20for%20security).

In summary, extensions greatly expand DuckDB’s versatility. They can be loaded on the fly and used within your SQL queries. If you want certain extensions always available (e.g., `httpfs` for cloud access), you can load them at connection time or even set an environment variable like `DUCKDB_INSTALL_ALL_EXTENSIONS=1` to pre-install all official extensions[\[39\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Each%20extension%20typically%20has%20an,install%20all%20known%20ones). Just be mindful of security when auto-downloading code; only use extensions from trusted sources.

### Attaching and Using Multiple Databases

A single DuckDB **connection** can operate on multiple databases simultaneously using the `ATTACH` command. This advanced feature (similar to SQLite’s ATTACH) allows you to attach additional DuckDB database files or even other database types, enabling cross-database queries and data transfer[\[40\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=ATTACH%20and%20Multiple%20Databases).

Using `ATTACH`, you can bring another DuckDB file into the current connection’s scope under a logical name (alias):

    ATTACH 'sales.duckdb' AS sales_db;
    ATTACH 'analytics.duckdb' AS analytics_db (READ_ONLY);

After these commands (executed via `conn.execute(...)` in Python), the current connection has three databases: the main database (the one initially opened, often unnamed or called `'main'`), and two attached ones named `sales_db` and `analytics_db`. You can list all databases with `SHOW DATABASES;`[\[41\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=tables%20are%20no%20longer%20accessible,in%20the%20main%20connection). To **detach** a database, use `DETACH sales_db;` when you no longer need it[\[42\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Inferring%20alias%3A%20If%20you%20do,96).

Once attached, you can reference tables in those databases by prefixing the alias. For example, to join a table from `sales_db` with a table from `analytics_db`:

    SELECT s.customer_id, s.amount, a.segment
    FROM sales_db.sales_table AS s
    JOIN analytics_db.customer_segments AS a 
      ON a.id = s.customer_id;

This query (which can be run via `conn.sql(...)`) combines data across two DuckDB files[\[43\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=This%20attaches%20two%20database%20files%2C,database%20queries). DuckDB’s query engine will treat it as one query, pulling data from both sources as needed. You can attach **any number of databases** in this manner. By default, attached databases are writable; you can specify `(READ_ONLY)` to prevent modifications on an attached file[\[44\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Here%20sales_db,only%5B94%5D%5B95).

If you omit the `AS name` part, DuckDB will derive an alias from the filename. For example `ATTACH 'data.duckdb';` would attach the database with alias `data`[\[45\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=only). You can also switch the *default* database context with `USE analytics_db;` so that unqualified table names refer to that attached database by default[\[46\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Listing%20databases%3A%20SHOW%20DATABASES%3B%20will,98) (you can always still refer to others with explicit prefixes).

**Attaching Remote Databases:** If your DuckDB file is on the cloud (S3, etc.), you can attach it via a URL. For example:

    ATTACH 's3://my-bucket/warehouse.duckdb' AS wh_db (READ_ONLY);

With `httpfs` enabled and appropriate credentials, DuckDB will access the remote DuckDB file and attach it[\[47\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=You%20can%20also%20attach%20remote,DuckDB%20files%20via%20URLs). Remote attachments are typically read-only. This allows you to query data in a cloud-stored DuckDB database without downloading it fully.

**Attaching Other Database Systems:** Through extensions, DuckDB can attach non-DuckDB databases. For instance, after loading the `sqlite` extension (`LOAD 'sqlite';`), you can attach a SQLite database file:

    ATTACH 'legacy.db' AS legacy_db (TYPE sqlite);

This mounts the SQLite database under the name `legacy_db`[\[37\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Additionally%2C%20ATTACH%20supports%20attaching%20non,databases%20via%20extensions). Now you can perform SQL queries that join DuckDB tables with SQLite tables, etc. Similarly, with the `postgres` or `mysql` extensions, you can connect to those systems (with appropriate connection parameters) and query them via DuckDB as if they were attached databases[\[36\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Other%20extensions%3A%20DuckDB%20has%20many,etc)[\[37\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Additionally%2C%20ATTACH%20supports%20attaching%20non,databases%20via%20extensions). DuckDB becomes a federated query engine in this setup.

**Moving Data Between Databases:** Attaching multiple DuckDB files makes it easy to copy data between them. You can do:

- **CREATE TABLE ... AS SELECT:** for example, `CREATE TABLE analytics_db.new_table AS SELECT * FROM main.old_table;` to copy a table from the main database to an attached one.
- **INSERT INTO ... SELECT:** to append data from one DB to a table in another.
- **COPY FROM DATABASE:** DuckDB 0.5.0+ introduced a command to bulk copy all tables from one database to another: `COPY FROM DATABASE source_db TO target_db;`[\[48\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Data%20transfer%20between%20DBs%3A%20Attaching,2%2B%20introduced%20COPY%20FROM%20DATABASE). This will transfer every table (optionally filtered by a list or prefix) from the source to target database in one go, which can be very convenient for migrations[\[49\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=AS%20SELECT%20,2%2B%20introduced%20COPY%20FROM%20DATABASE).

**Encrypted Databases:** DuckDB supports transparent **encryption** of database files. If a DuckDB file was created with encryption, you need to provide the key on attach. For example:

    ATTACH 'secret.duckdb' AS secret_db (ENCRYPTION_KEY 'mySecretPwd');

This will attach `secret.duckdb` using the provided key to decrypt/encrypt data on the fly[\[50\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Encryption%3A%20DuckDB%20supports%20transparent%20encryption,you%20supply%20an%20encryption%20key). You can also create a new encrypted database by attaching a new file with an `ENCRYPTION_KEY` and then copying data into it. (Note: encryption support requires DuckDB to be built with encryption enabled. The `httpfs` extension accelerates encryption by leveraging OpenSSL[\[51\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=http%3A%2F%2F%20or%20s3%3A%2F%2F%20path,you%20want%20to%20be%20sure).)

### Exporting and Importing Databases

For backup, migration, or upgrade purposes, DuckDB can **export** an entire database (all its schemas and tables) to a set of files, and later **import** those files to recreate the database. This is useful for upgrading to a new DuckDB version (as the storage format can change) or transferring data to another environment.

- **Export**: Use the SQL command `EXPORT DATABASE 'directory_path' (FORMAT ...)`. This will create the specified directory and dump the current database’s contents into it[\[52\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=EXPORT%20DATABASE%20%27dir_path%27%20%28FORMAT%20,111%5D.%20Example). By default, `FORMAT CSV` is used: each table is saved as a CSV file, and additional files `schema.sql` (with CREATE TABLE statements) and `load.sql` (with COPY commands to load the CSVs) are written[\[52\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=EXPORT%20DATABASE%20%27dir_path%27%20%28FORMAT%20,111%5D.%20Example). You can also specify `FORMAT PARQUET` to export tables as Parquet files instead[\[52\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=EXPORT%20DATABASE%20%27dir_path%27%20%28FORMAT%20,111%5D.%20Example)[\[53\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=recreate%20schema%20and%20load,111%5D.%20Example). For example:

<!-- -->

    conn.execute("EXPORT DATABASE 'backup_dir' (FORMAT parquet)")

This will produce a `backup_dir/` containing all table data as Parquet files, plus SQL scripts for schema and load instructions[\[53\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=recreate%20schema%20and%20load,111%5D.%20Example). Options like compression (e.g., `COMPRESSION ZSTD`) and row group size can be given when exporting to Parquet[\[54\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=database%20,111%5D.%20Example). The export process is transactionally safe and will snapshot the data as of the time of the command.

- **Import**: To load a previously exported database, use `IMPORT DATABASE 'directory_path'` on a fresh connection (with an empty or new database)[\[55\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=IMPORT%20DATABASE%20%27dir_path%27%20does%20the,115). DuckDB will read the `schema.sql` and `load.sql` in that directory and execute them, recreating all tables and loading the data[\[55\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=IMPORT%20DATABASE%20%27dir_path%27%20does%20the,115). Equivalently, you can manually run those SQL scripts. There is also a convenience pragma: `PRAGMA import_database('directory_path');` which does the same in one call[\[55\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=IMPORT%20DATABASE%20%27dir_path%27%20does%20the,115). After import, the database should be identical to the exported one.

This export/import approach is the recommended way to upgrade a DuckDB database to a newer version of DuckDB that isn’t backward-compatible: you **export** using the old version, then connect with the new version and **import**[\[56\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=These%20commands%20are%20useful%20for,readable%20schema%20and%20data%20files). It’s also a handy mechanism for backups or for moving a subset of data (by exporting only certain tables or filtering in the export). Keep in mind that exporting to CSV is portable but can be slower and larger, whereas Parquet format is faster and more efficient for large data (and retains data types). Always verify after import that all expected objects are present.

## 1.6 Relational API Advanced Functionality

DuckDB’s **Relational API** allows you to build and manipulate queries using Python objects (DuckDB relations) instead of writing raw SQL. Beyond the basic usage (chaining filters, projections, joins, etc.), there are advanced features that integrate DuckDB’s relational engine tightly with Python data and other systems. These capabilities provide a *seamless bridge* between in-memory data (pandas/Polars/NumPy) and DuckDB, enabling complex workflows without unnecessary data movement.

### Querying In-Memory Data Structures (Pandas, Polars, Arrow, NumPy)

One powerful feature of DuckDB is the ability to run SQL directly on **Python in-memory data structures** *as if they were tables*. This is done via **replacement scans**: when you refer to a Python variable in a DuckDB query, DuckDB will attempt to “replace” that table reference with the data contained in the Python object[\[57\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=DuckDB%20will%20detect%20that%20df,scan%20of%20the%20DataFrame%E2%80%99s%20data)[\[58\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Supported%20in,58). This works for several data types:

- **Pandas DataFrame** (`pandas.DataFrame`) – DuckDB can scan it as a table[\[57\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=DuckDB%20will%20detect%20that%20df,scan%20of%20the%20DataFrame%E2%80%99s%20data).
- **Polars DataFrame or LazyFrame** (`polars.DataFrame`, `polars.LazyFrame`)[\[58\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Supported%20in,58).
- **PyArrow Table / RecordBatch / Dataset** – scanned via Arrow’s zero-copy interface[\[58\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Supported%20in,58).
- **NumPy arrays** – 1D or 2D NumPy arrays (or structured arrays) can be treated as tables (DuckDB will map them to one or more columns)[\[58\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Supported%20in,58)[\[59\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=scan%20mechanism).
- **DuckDB Relation** (`DuckDBPyRelation`) – you can use an existing relation as a subquery in another query.
- **Python lists** (for simple cases) – small lists can be turned into DuckDB sequences or tables on the fly (e.g., a list of tuples).

When you run a query like `conn.sql("SELECT * FROM df")` and `df` is a pandas DataFrame in your Python session, DuckDB will transparently recognize that `df` refers to a DataFrame and will read from it directly[\[57\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=DuckDB%20will%20detect%20that%20df,scan%20of%20the%20DataFrame%E2%80%99s%20data). **No explicit import or temporary table creation is required**. For example:

    import pandas as pd
    df = pd.DataFrame({"i": [1, 2, 3], "j": ["a", "b", "c"]})

    # Query the Pandas DataFrame directly:
    result = conn.sql("SELECT * FROM df").fetchall()
    print(result)  
    # Output: [(1, 'a'), (2, 'b'), (3, 'c')]

DuckDB detected the DataFrame `df` in the query and performed a **replacement scan** on it[\[57\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=DuckDB%20will%20detect%20that%20df,scan%20of%20the%20DataFrame%E2%80%99s%20data), treating its contents as if they were a DuckDB table. This works similarly for Polars DataFrames, Arrow tables, etc. For Polars, DuckDB leverages Arrow interoperability under the hood: it can accept a Polars DataFrame and scan it using Arrow’s memory format, which is highly efficient (zero-copy)[\[60\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Polars%20integration%3A%20Similarly%2C%20if%20you,the%20same%20way%20by%20name)[\[61\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=duckdb.sql%28,the%20same%20way%20by%20name). In fact, **DuckDB’s Arrow integration is zero-copy**, meaning it can use Arrow memory directly without converting data, for both Arrow Tables and Arrow-backed structures like Polars[\[60\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Polars%20integration%3A%20Similarly%2C%20if%20you,the%20same%20way%20by%20name). This makes queries on those structures very fast.

**Supported object summary:** If a Python variable in the query name matches one of the supported types (pandas, Polars, Arrow, NumPy, etc.), DuckDB will use its data. The list of supported in-memory objects includes DataFrames, Arrow Datasets, and even DuckDB relations themselves[\[58\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Supported%20in,58). (As an edge case, if you have a Python variable that DuckDB doesn’t know how to scan, it will error that the table does not exist.)

**Name Resolution and Conflicts:** When DuckDB sees a name in a SQL query, it resolves it in this order of precedence[\[62\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Name%20precedence%3A%20If%20a%20name,you%20override%20existing%20tables%20safely):

1.  **Registered objects** (see below) – highest precedence if you explicitly registered a name.
2.  **Existing DuckDB tables/views** in the connection’s catalog.
3.  **Python variables by that name** (replacement scan)[\[62\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Name%20precedence%3A%20If%20a%20name,you%20override%20existing%20tables%20safely).

This means if you have a table in DuckDB named `df` and also a DataFrame `df` in Python, by default DuckDB will use the DuckDB table (since it checks its own tables before looking at Python variables). If you want to force using the Python object, you can either name it differently or use the registration mechanism described next.

**Explicit Object Registration:** In cases where the object isn’t a global variable or you want to manage the name, you can explicitly **register** a pandas/Arrow object as a DuckDB view using `duckdb.register(name, object)`. This gives the object a relation name in DuckDB’s catalog:

    duckdb.register('my_df_view', df)
    # Now 'my_df_view' can be queried directly in SQL
    result = conn.sql("SELECT * FROM my_df_view").to_df()

This creates a **temporary view** in DuckDB that aliases to the DataFrame’s data[\[63\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=or%20is%20out%20of%20scope%2C,you%20can%20explicitly%20register%20it). Registered names take highest precedence in resolution, so this is also a way to override a table name. Registration is useful if the DataFrame is not a global variable (e.g., inside a function) or if you want to use a shorter/more convenient name in SQL. You can register multiple dataframes (each with a unique view name), and they will persist for the life of the connection or until you unregister them.

After you’re done with a registered object or if you want to update it, you can simply register again (overwriting the name) or use `conn.unregister('my_df_view')` (DuckDB might also unregister on connection close). Under the hood, registering creates a pointer to the Python object; DuckDB will fetch the data from the object only when the table is accessed in a query.

**Querying Polars and Arrow:** As noted, Polars DataFrames and LazyFrames can be queried by name just like pandas DataFrames[\[60\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Polars%20integration%3A%20Similarly%2C%20if%20you,the%20same%20way%20by%20name). DuckDB will convert Polars data to Arrow and then read it. You can even query a **Polars LazyFrame** directly – DuckDB will execute the LazyFrame (materialize it via Arrow) only when needed. For Arrow, if you have a `pyarrow.Table`, you can do:

    import pyarrow as pa
    table = pa.table({'x': [1, 2, 3]})
    res = conn.sql("SELECT SUM(x) FROM table").fetchone()
    # DuckDB reads the Arrow table directly

This works because DuckDB sees `table` and recognizes it as a `pyarrow.Table` in the Python scope[\[64\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=can%20be%20queried%20the%20same,way%20by%20name). It then reads from that Arrow table as if it were an internal table. Since DuckDB uses the Arrow C++ interface internally, this scan is very fast and doesn’t copy data[\[65\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Arrow%20objects%3A%20If%20you%20use,For%20example).

For **NumPy arrays**, DuckDB can treat a structured array or a 2D array as a table. If you have a 2D NumPy array without named dtypes, DuckDB will assign generic column names. For more complex or ragged arrays, it's often easier to convert them to a DataFrame or Arrow table first. NumPy support is best for basic cases (e.g., a 2D numeric array can be seen as two columns).

**Bringing Query Results into Python:** We’ve seen that calling `.to_df()`, `.fetchall()`, etc., on a relation or query will execute it and bring results into Python. In addition, DuckDB’s relation objects have methods to directly produce other formats (if not already covered earlier): for example, `relation.to_arrow()` gives a PyArrow Table, `relation.fetchnumpy()` yields NumPy arrays, `relation.pl()` yields a Polars DataFrame, `relation.torch()` gives PyTorch tensors, etc. These were discussed in an earlier section, but it’s worth noting in the context of working with in-memory data: DuckDB makes it easy not only to query Python objects in SQL, but also to get results back into the Python objects of your choice (pandas, Arrow, Polars, NumPy, etc.)[\[66\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Arrow%20Table%3A%20Use%20relation,24%5D.%20For%20example)[\[67\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=NumPy%3A%20Use%20relation,numpy.ndarray%5B27%5D.%20For%20example). The interchange is quite flexible.

### Persisting In-Memory Data and Interop Pitfalls

When you query a Python object (say a pandas DataFrame) via DuckDB, you might want to **persist** that data inside DuckDB—for example, to join with other tables or to reuse without keeping the pandas object around. DuckDB makes this simple:

- Use `CREATE TABLE AS ... SELECT` to materialize a Python-sourced relation into a real DuckDB table on disk (or in the in-memory database). For instance: `conn.execute("CREATE TABLE mytable AS SELECT * FROM df")`. This will read all data from the DataFrame `df` and write it into a new DuckDB table named `mytable`[\[68\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=After%20querying%20a%20DataFrame%2FArrow%2C%20you,Failed%20to%20cast%20value).
- If the table already exists, you can `INSERT INTO ... SELECT * FROM df` to append data from the DataFrame into it[\[68\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=After%20querying%20a%20DataFrame%2FArrow%2C%20you,Failed%20to%20cast%20value). For example, `conn.execute("INSERT INTO mytable SELECT * FROM df")` takes each row of `df` and inserts it. Under the hood, DuckDB fetches the DataFrame in chunks and converts data efficiently[\[69\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=TABLE%20mytable%20AS%20SELECT%20,Failed%20to%20cast%20value).

Because DuckDB streams data in chunks when pulling from a DataFrame, it can handle large DataFrames without needing to have the entire dataset as a single Python object in memory (it will fetch, e.g., 1000 rows at a time by default). One thing to be aware of is **type inference**: when DuckDB reads a pandas DataFrame, columns with Python objects (dtype `object`, often used for strings or mixed types) require type guessing. DuckDB by default will sample the first 1000 rows to infer the column type (e.g., all values are int vs. float vs. string)[\[70\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=TABLE%20mytable%20AS%20SELECT%20,or). If your DataFrame has mixed types in a column or the first 1000 rows aren’t representative, this can lead to type inference issues or errors like *“Failed to cast value”*. To address this, you can increase the sample size DuckDB uses for type inference with a configuration parameter. For example:

    SET pandas_analyze_sample=10000;

This tells DuckDB to examine 10,000 rows for type inference instead of 1000[\[71\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Similarly%2C%20if%20mytable%20exists%2C%20conn.execute%28,66). You might set an even higher value or `'ALL'` to scan the entire column for type inference if needed, at the cost of some performance on the initial analysis[\[71\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Similarly%2C%20if%20mytable%20exists%2C%20conn.execute%28,66). Setting this before a `CREATE TABLE AS SELECT * FROM df` or similar command can help avoid type cast failures for inconsistent data. Another approach is to manually ensure your DataFrame’s dtypes are appropriate (e.g., convert columns to string or float in pandas before querying, if they contain mixed types).

**Example – Combining Pandas and DuckDB:** To illustrate the in-memory query and persistence, consider:

    # Assume conn is an open DuckDB connection
    import pandas as pd

    sales = pd.DataFrame({
        "region": ["US", "EU", "US"],
        "product": ["A", "A", "B"],
        "revenue": [100, 150, 200]
    })

    # Query the Pandas DataFrame directly with DuckDB (group by region):
    result = conn.sql("""
        SELECT region, SUM(revenue) AS total_rev
        FROM sales
        GROUP BY region
    """).to_df()

    print(result)
    # Output:
    #   region  total_rev
    # 0     EU        150
    # 1     US        300

In this query, `sales` is a pandas DataFrame used as if it were a table in DuckDB’s SQL[\[72\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=200%5D%20,to_df%28%29%20print%28result)[\[73\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=0%20%20%20EU%20,300). DuckDB scans the DataFrame and performs the aggregation entirely in its engine. The result is then converted to a pandas DataFrame (via `.to_df()`), which shows the total revenue per region. We did not need to load `sales` into DuckDB beforehand — the integration is on-the-fly.

If we wanted to reuse this data in more queries, we could do `conn.execute("CREATE TABLE sales_data AS SELECT * FROM sales")`. That would create a DuckDB table `sales_data` containing the same three rows, which persists for the rest of the session (or on disk if using a persisted database)[\[68\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=After%20querying%20a%20DataFrame%2FArrow%2C%20you,Failed%20to%20cast%20value). We could then join `sales_data` with other DuckDB tables, index it, etc., without referring to the pandas DataFrame again.

This tight integration between DuckDB and in-memory dataframes means you can **treat your Python data and database data uniformly**. You might load a dataset in pandas for preprocessing, then register it and use DuckDB SQL to analyze it or join with large Parquet files, all in one fluid workflow. The relational API’s lazy nature also means these operations are efficient: DuckDB will only pull the minimal data needed (thanks to predicate pushdown, projection pushdown etc., even against dataframes/Arrow).

### Additional Relational API Features

Beyond the basics, DuckDB’s `DuckDBPyRelation` offers various methods to refine and optimize queries:

- **Aggregation and Sorting:** Methods like `relation.aggregate(agg_expr, group_expr)` perform grouped aggregations (internally using SQL `GROUP BY`) and `relation.order(order_expr)` sorts the results (SQL `ORDER BY`)[\[74\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=relation.filter%28,13)[\[75\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=relation.aggregate%28,GROUP%20BY). These can be chained with filters and joins to build complex queries step-by-step. For example: `rel = rel.filter("score > 50").aggregate("AVG(score)", "country")` would compute average score per country on rows with score \> 50.

- **Set Operations:** While not illustrated above, the relation API does support set operations analogous to SQL UNION, EXCEPT, INTERSECT via methods or by using the `.union()`, `.except_()`, `.intersect()` (if available in the API). If such methods are not in the Python API, you can always fall back to a SQL call like `rel_a.union(rel_b)` by converting relations to SQL strings or using `conn.sql()` with subqueries.

- **Subqueries and Combining Relations:** You can use one `DuckDBPyRelation` as part of another by treating it like a table. For instance, `conn.sql("SELECT * FROM ", rel)` will interpolate the relation as a subquery. More directly, you can pass a relation to the `.join()` method of another relation as the right side, as we saw earlier in basic usage (e.g., `relation.join(other_relation, "condition")`). This allows you to build a query from multiple data sources incrementally.

- **Materializing or Viewing Relations:** We showed `relation.create("table_name")` earlier to materialize a relation’s result as a new table, and `relation.create_view("view_name")` to save it as a SQL view[\[76\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Materialize%20as%20Table%2FView%3A%20Use%20relation.create%28,33%5D.%20For%20example). These are handy for breaking down complex transformations or caching intermediate results. For example, if you have a long chain of operations, you might create a view for a common subquery and then query that view in subsequent steps (this can sometimes help DuckDB’s optimizer, or just make logic clearer).

- **Lazy Execution Benefits:** Remember that DuckDB relations are **lazily evaluated** – none of the transformations actually run the query until you request output (via `.fetchall()`, `.to_df()`, `.show()`, etc.)[\[77\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=At%20this%20point%2C%20rel%20is,Triggers%20include)[\[78\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Calling%20rel,and%20bring%20results%20into%20Python). This means you can freely compose and recompose query pieces without incurring query cost each step. DuckDB will combine them into one optimized query execution plan. From a system perspective, this allows global optimizations (like pushing filters down to file scans or reordering joins) that wouldn’t be possible if each step executed immediately. Leverage this by building queries modularly. If you do need an intermediate result for re-use, explicitly materialize it as described.

Finally, using the **relational API vs. SQL** is a matter of preference. Anything you can do with `DuckDBPyRelation` methods can be done with SQL, and vice versa. The relational API is especially convenient when integrating with pandas or building queries dynamically in Python. It also provides type safety and auto-completion in editors, since you are working with Python objects. But it’s perfectly fine to mix and match – e.g., use the relational API to get a base relation from a DataFrame, then use `.sql()` to run a custom SQL snippet on it. DuckDB is flexible in letting you go back and forth between the two interfaces.

In summary, the advanced relational API functionality in DuckDB’s Python client allows a Python expert to **seamlessly blend in-memory computing with SQL**. You can treat dataframes as tables, define custom Python logic as SQL functions, attach external data sources, and orchestrate everything through familiar Python constructs. This opens up a wide range of possibilities: from quick data exploration to building an entire data pipeline that joins Python data with large on-disk datasets, all optimized by DuckDB’s engine. With these tools, you have a unified environment to manage data and perform analytics with minimal friction. Happy querying\![\[79\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=This%20concludes%20the%20advanced%20reference,Happy%20querying)[\[80\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=,Relational%20API%20%E2%80%93%20DuckDB)

[\[1\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=The%20API%20follows%20DB) [\[2\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Use%20%3F%20placeholders%20in%20your,68) [\[3\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=conn.execute%28,e.g.%2C%20%5B%28%27laptop%27%2C%29%2C%20%28%27chainsaw) [\[4\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=In%20the%20above%3A%20,it%20for%20each%20parameter%20list) [\[5\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Here%20%24greet%20and%20%24noun%20in,will%20use%20the%20same%20value) [\[6\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=occurrence%20of%20%24name%20will%20use,the%20same%20value) [\[7\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=DB,74) [\[8\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=property%20gives%20column%20names%20of,74) [\[9\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Note%20on%20performance%3A%20DuckDB%20can,77) [\[10\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=DuckDB%20allows%20you%20to%20define,leveraging%20DuckDB%20for%20data%20access) [\[11\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Vectorized%20,For%20example) [\[12\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Here%2C%20DuckDB%20calls%20vector_add%20only,be%20directly%20used%20or%20converted) [\[13\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Use%20conn,for%20large%20data%20due%20to) [\[14\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=the%20actual%20Python%20callable%20%28e,for%20large%20data%20due%20to) [\[15\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=%28e,83%5D.%20If%20you%20want) [\[16\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=function%20for%20each%20value,you%20can%20decide%20what%20to) [\[17\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=conn.create_function%28,instead%20of%20stopping%20the%20query) [\[18\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Use%20conn,for%20large%20data%20due%20to) [\[19\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=annotations%2C%20you%20can%20often%20pass,set%20null_handling%3D%27special%27%20%E2%80%93%20then%20your) [\[20\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=def%20add_ten%28x%3A%20int%29%20,return%20x%20%2B%2010) [\[21\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=import%20numpy%20as%20np%20def,elementwise%20addition) [\[22\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Resource%20management%3A%20Python%20UDFs%20execute,are%20invaluable%20for%20custom%20logic) [\[23\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=unregister%20the%20function%20from%20the,connection%E2%80%99s%20catalog) [\[24\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=DuckDB%E2%80%99s%20functionality%20can%20be%20extended,cover%20connectivity%20and%20file%20formats) [\[25\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=httpfs%3A%20This%20extension%20enables%20DuckDB,you%20want%20to%20be%20sure) [\[26\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Installing%2FLoading%20Extensions%3A%20DuckDB%20has%20an,you%20can%20also%20explicitly%20install%2Fload) [\[27\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=conn.execute%28%22INSTALL%20%27json%27%3B%22%29%20%20%20,install%20if%20needed) [\[28\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=For%20Python%2C%20many%20extensions%20,loading%20is%20disabled) [\[29\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=json%3A%20The%20JSON%20extension%20provides,functions%20for%20querying%20JSON%20structures) [\[30\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=excel%3A%20The%20Excel%20extension%20allows,To%20use) [\[31\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=conn.execute%28) [\[32\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=auto,not%20present) [\[33\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=delta%3A%20The%20Delta%20Lake%20extension,Usage) [\[34\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=SELECT%20) [\[35\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=iceberg%3A%20The%20Iceberg%20extension%20allows,need%20to%20configure%20a%20catalog) [\[36\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Other%20extensions%3A%20DuckDB%20has%20many,etc) [\[37\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Additionally%2C%20ATTACH%20supports%20attaching%20non,databases%20via%20extensions) [\[38\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Keep%20in%20mind%20that%20community%2Fthird,install%20for%20security) [\[39\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Each%20extension%20typically%20has%20an,install%20all%20known%20ones) [\[40\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=ATTACH%20and%20Multiple%20Databases) [\[41\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=tables%20are%20no%20longer%20accessible,in%20the%20main%20connection) [\[42\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Inferring%20alias%3A%20If%20you%20do,96) [\[43\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=This%20attaches%20two%20database%20files%2C,database%20queries) [\[44\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Here%20sales_db,only%5B94%5D%5B95) [\[45\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=only) [\[46\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Listing%20databases%3A%20SHOW%20DATABASES%3B%20will,98) [\[47\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=You%20can%20also%20attach%20remote,DuckDB%20files%20via%20URLs) [\[48\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Data%20transfer%20between%20DBs%3A%20Attaching,2%2B%20introduced%20COPY%20FROM%20DATABASE) [\[49\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=AS%20SELECT%20,2%2B%20introduced%20COPY%20FROM%20DATABASE) [\[50\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Encryption%3A%20DuckDB%20supports%20transparent%20encryption,you%20supply%20an%20encryption%20key) [\[51\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=http%3A%2F%2F%20or%20s3%3A%2F%2F%20path,you%20want%20to%20be%20sure) [\[52\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=EXPORT%20DATABASE%20%27dir_path%27%20%28FORMAT%20,111%5D.%20Example) [\[53\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=recreate%20schema%20and%20load,111%5D.%20Example) [\[54\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=database%20,111%5D.%20Example) [\[55\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=IMPORT%20DATABASE%20%27dir_path%27%20does%20the,115) [\[56\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=These%20commands%20are%20useful%20for,readable%20schema%20and%20data%20files) [\[57\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=DuckDB%20will%20detect%20that%20df,scan%20of%20the%20DataFrame%E2%80%99s%20data) [\[58\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Supported%20in,58) [\[59\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=scan%20mechanism) [\[60\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Polars%20integration%3A%20Similarly%2C%20if%20you,the%20same%20way%20by%20name) [\[61\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=duckdb.sql%28,the%20same%20way%20by%20name) [\[62\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Name%20precedence%3A%20If%20a%20name,you%20override%20existing%20tables%20safely) [\[63\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=or%20is%20out%20of%20scope%2C,you%20can%20explicitly%20register%20it) [\[64\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=can%20be%20queried%20the%20same,way%20by%20name) [\[65\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Arrow%20objects%3A%20If%20you%20use,For%20example) [\[66\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Arrow%20Table%3A%20Use%20relation,24%5D.%20For%20example) [\[67\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=NumPy%3A%20Use%20relation,numpy.ndarray%5B27%5D.%20For%20example) [\[68\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=After%20querying%20a%20DataFrame%2FArrow%2C%20you,Failed%20to%20cast%20value) [\[69\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=TABLE%20mytable%20AS%20SELECT%20,Failed%20to%20cast%20value) [\[70\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=TABLE%20mytable%20AS%20SELECT%20,or) [\[71\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Similarly%2C%20if%20mytable%20exists%2C%20conn.execute%28,66) [\[72\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=200%5D%20,to_df%28%29%20print%28result) [\[73\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=0%20%20%20EU%20,300) [\[74\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=relation.filter%28,13) [\[75\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=relation.aggregate%28,GROUP%20BY) [\[76\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Materialize%20as%20Table%2FView%3A%20Use%20relation.create%28,33%5D.%20For%20example) [\[77\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=At%20this%20point%2C%20rel%20is,Triggers%20include) [\[78\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=Calling%20rel,and%20bring%20results%20into%20Python) [\[79\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=This%20concludes%20the%20advanced%20reference,Happy%20querying) [\[80\]](file://file-C6naX8tRxEh6rmu1Bza6tk#:~:text=,Relational%20API%20%E2%80%93%20DuckDB) DuckDB Python Client Advanced Reference (v1.4+).docx

<file://file-C6naX8tRxEh6rmu1Bza6tk>
