const std = @import("std");
const testing = std.testing;
const chdb = @import("chdb_zig_lib");

test "ChConn - basic DDL execution" {
    const allocator = testing.allocator;
    const conn = try chdb.ChConn.init(allocator, "");
    // defer conn.deinit();

    _ = try conn.exec(@constCast("DROP TABLE IF EXISTS test"), .{});
    const result = try conn.exec(@constCast("CREATE TABLE test (id Int32, name String) ENGINE=MergeTree() ORDER BY id"), .{});
    try testing.expect(@as(u64, 0) == result.rows_read);
}

test "ChConn - insert with parameters" {
    const allocator = testing.allocator;
    const conn = try chdb.ChConn.init(allocator, "");
    defer conn.deinit();

    _ = try conn.exec(@constCast("DROP TABLE IF EXISTS test"), .{});
    _ = try conn.exec(@constCast("CREATE TABLE test (id Int32, name String) ENGINE=MergeTree() ORDER BY id"), .{});

    const result = try conn.exec(@constCast("INSERT INTO test VALUES ({i}, {s}), ({i}, {s})"), .{ 1, "one", 2, "two" });
    try testing.expect(@as(u64, 2) == result.rows_read);
}

test "ChConn - query with results" {
    const allocator = testing.allocator;
    const conn = try chdb.ChConn.init(allocator, "");
    defer conn.deinit();

    _ = try conn.exec(@constCast("DROP TABLE IF EXISTS test"), .{});
    _ = try conn.exec(@constCast("CREATE TABLE test (id Int32, name String) ENGINE=MergeTree() ORDER BY id"), .{});
    _ = try conn.exec(@constCast("INSERT INTO test VALUES ({i}, {s}), ({i}, {s})"), .{ 1, "one", 2, "two" });

    var query_result = try conn.query(@constCast("SELECT * FROM test ORDER BY id"), .{});
    defer query_result.free();

    var count: usize = 0;
    while (query_result.next()) |row| {
        defer row.deinit(); // Free each row after use

        const id = row.get(i32, "id");
        const name = row.get([]const u8, "name");

        switch (count) {
            0 => {
                try testing.expect(id.? == 1);
                try testing.expectEqualStrings("one", name.?);
            },
            1 => {
                try testing.expect(id.? == 2);
                try testing.expectEqualStrings("two", name.?);
            },
            else => unreachable,
        }
        count += 1;
    }
    try testing.expect(count == 2);
}

test "ChConn - error handling invalid SQL" {
    const allocator = testing.allocator;
    const conn = try chdb.ChConn.init(allocator, "");
    defer conn.deinit();

    const result = conn.exec(@constCast("INVALID SQL STATEMENT"), .{});
    try testing.expectError(error.SqlError, result);
}

test "ChConn - error handling invalid table" {
    const allocator = testing.allocator;
    const conn = try chdb.ChConn.init(allocator, "");
    defer conn.deinit();

    const result = conn.query(@constCast("SELECT * FROM nonexistent_table"), .{});
    try testing.expectError(error.SqlError, result);
}

test "ChConn - query with parameters" {
    const allocator = testing.allocator;
    const conn = try chdb.ChConn.init(allocator, "");
    defer conn.deinit();

    _ = try conn.exec(@constCast("DROP TABLE IF EXISTS test"), .{});
    _ = try conn.exec(@constCast("CREATE TABLE test (id Int32, name String) ENGINE=MergeTree() ORDER BY id"), .{});
    _ = try conn.exec(@constCast("INSERT INTO test VALUES ({i}, {s})"), .{ 1, "test" });

    var query_result = try conn.query(@constCast("SELECT * FROM test WHERE id = {i}"), .{1});
    defer query_result.free();

    if (query_result.next()) |row| {
        defer row.deinit(); // Free the row after use

        const id = row.get(i32, "id");
        const name = row.get([]const u8, "name");
        try testing.expect(id.? == 1);
        try testing.expectEqualStrings("test", name.?);
    } else {
        try testing.expect(false); // Should have found a row
    }
}
