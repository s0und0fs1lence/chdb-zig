# chdb-zig

A Zig binding for [chdb](https://github.com/chdb-io/chdb) - the embedded ClickHouse database engine. This library provides a safe and convenient way to interact with ClickHouse directly from Zig, leveraging the language's memory safety features and type system.

## Overview

chdb-zig wraps the C API of chdb, giving you access to a full-featured SQL database that runs in-process without needing to manage a separate server. Whether you need to query Parquet files, create in-memory tables, or perform complex analytical queries, chdb-zig makes it straightforward.

## Basic Usage

Here's a simple example that creates a table and queries it:

```zig
const std = @import("std");
const chdb_zig = @import("chdb_zig");

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .{};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    // Initialize a connection with options
    const options = chdb_zig.ChdbConnectionOptions{
        .UseMultiQuery = true,
        .Path = "my_database.db",
    };

    const conn = try chdb_zig.initConnection(allocator, options);
    defer conn.deinit();

    // Create a table
    try conn.execute(@constCast("CREATE TABLE IF NOT EXISTS test (id Int32, name String) " ++
        "ENGINE = MergeTree() ORDER BY id"));

    try conn.execute(@constCast("INSERT INTO test (id,name) VALUES (1,'Alice'), (2,'Bob')"));
    // Query the database
    var result = try conn.query(@constCast("SELECT * FROM test"));
    if (!result.isSuccess()) {
        std.debug.print("Query failed: {?s}\n", .{result.getError()});
        return;
    }
    defer result.deinit();

    // Iterate through results
    var iter = result.iter(allocator);
    while (iter.nextRow()) |row| {
        std.debug.print("Row: {s}\n", .{row});
    }
}
```

## Working with Remote Data

One of the powerful features of chdb is the ability to query remote data sources directly. Here's an example using Parquet files from a URL:

```zig
const query = 
    \\CREATE TABLE IF NOT EXISTS parquet_data ENGINE = MergeTree() 
    \\ORDER BY tuple()
    \\AS SELECT * FROM url('https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet');
;

try conn.execute(@constCast(query));

// Now query the data
var result = try conn.query(@constCast(
    "SELECT URL, COUNT(*) FROM parquet_data " ++
    "GROUP BY URL ORDER BY COUNT(*) DESC LIMIT 10"
));

// Access query statistics
std.debug.print("Elapsed time: {d}ms\n", .{result.elapsedTime()});
std.debug.print("Rows read: {d}\n", .{result.rowsRead()});
std.debug.print("Bytes read: {d}\n", .{result.bytesRead()});
```

## Connection Options

The `ChdbConnectionOptions` struct allows you to configure how the connection behaves:

- `UseMultiQuery` - Enable support for multiple queries in a single statement
- `Path` - File path for persistent storage (omit for in-memory database)
- `LogLevel` - Set logging verbosity (e.g., "debug", "info")
- `CustomArgs` - Pass additional command-line arguments to chdb

## API Overview

### Executing Queries

- `execute()` - Run a query and discard the result
- `query()` - Run a query and return results
- `queryStreaming()` - Stream large result sets

### Working with Results

After executing a query, you get a `ChdbResult` object with the following methods:

- `iter()` - Get an iterator over result rows (NDJSON format)
- `isSuccess()` - Check if the query succeeded
- `getError()` - Retrieve error message if query failed
- `elapsedTime()` - Query execution time
- `rowsRead()` - Number of rows processed
- `bytesRead()` - Number of bytes read
- `storageRowsRead()` - Rows read from disk
- `storageBytesRead()` - Bytes read from disk

## Memory Management

This library uses Zig's allocator pattern. You should always defer cleanup:

```zig
var gpa: std.heap.GeneralPurposeAllocator(.{}) = .{};
const allocator = gpa.allocator();
defer _ = gpa.deinit();

const conn = try chdb_zig.initConnection(allocator, options);
defer conn.deinit();

var result = try conn.query(@constCast(query));
defer result.deinit();
```

## Installation

### Add as a Dependency

Add chdb-zig to your `build.zig.zon`:

```zig
.dependencies = .{
    .chdb_zig = .{
        .url = "https://github.com/s0und0fs1lence/chdb-zig/archive/refs/tags/0.0.4.tar.gz",
        .hash = "12200c7a3c6b8e9f1d2a3b4c5d6e7f8g9h0i1j2k3l4m5n6o7p8q9r0s1t2u3v4w5x6y7z8",
    },
},
```

### Configure in build.zig

In your `build.zig`, add the dependency to your executable:

```zig
const chdb_dep = b.dependency("chdb_zig", .{
    .target = target,
    .optimize = optimize,
});

// Get the module from the dependency
const chdb_module = chdb_dep.module("chdb_zig");
chdb_module.link_libc = true;

// Add the module to your executable's imports
exe.root_module.addImport("chdb_zig", chdb_module);
```

Now you can import and use chdb-zig in your code:

```zig
const chdb_zig = @import("chdb_zig");
```

## Contributing

Contributions are welcome. Feel free to open issues or submit pull requests.

## License

Licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for details.
