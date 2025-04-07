# chdb-zig

A Zig wrapper for [chdb](https://github.com/chdb-io/chdb) - the embedded ClickHouse database. This library provides a safe, efficient, and ergonomic way to interact with chdb using Zig's powerful type system and memory safety features.

## Features

- 🛡️ Base sql interpolation function to be able to pass arguments to query
- 🚀 Zero-allocation query building where possible
- 🎯 Type-safe query parameter interpolation
- 📦 Native Zig implementation
- ⚡ Direct chdb integration


## Usage

```zig
const std = @import("std");
const chdb = @import("chdb");

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    
    const conn = chdb.ChConn.init(alloc, ":memory:") catch |err| {
        std.debug.print("Error: {}\n", .{err});
        return err;
    };

    defer conn.deinit();

    var result = try conn.exec(@constCast("CREATE TABLE test (id Int32) engine=MergeTree() order by id;"), .{}); 

    std.debug.print("{d}\n", .{result.affectedRows()});

    result = try conn.exec(@constCast("INSERT INTO test values (1),(2),(3)"), .{});

    std.debug.print("{d}\n", .{result.affectedRows()});

    const res = try conn.query(@constCast("select * from test"), .{});

    while (res.next()) |row| {
       
        const columns = row.columns();
        for (columns) |column| {
            std.debug.print("{s}\n", .{column});
        }
        const val: ?i64 = row.get(i64, "id");
        std.debug.print("{d}\n", .{val.?});
    }
}
```

## Installation

Coming soon!


## Features

### SQL Interpolation
- Type-safe parameter binding
- Protection against SQL injection (very basic and not a replacement for proper sanitization)
- Support for:
  - Strings (escaped)
  - Integers (signed/unsigned)
  - Floats
  - Dates and DateTimes
  - Arrays
  - Boolean values
  - NULL values

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.
