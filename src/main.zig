//! By convention, main.zig is where your main function lives in the case that
//! you are building an executable. If you are making a library, the convention
//! is to delete this file and start with root.zig instead.
const libChdb = @import("lib.zig");
const std = @import("std");
const sql_interpolator = @import("sql_interpolator.zig");

const TestStruct = struct { id: i32 };

pub fn main() !void {
    const alloc = std.heap.smp_allocator;

    const conn = libChdb.ChConn.init(alloc, "--path=/tmp/chdb&readonly=1") catch |err| {
        std.debug.print("Error: {}\n", .{err});
        return err;
    };

    defer conn.deinit();

    var result = try conn.exec(@constCast("CREATE TABLE test (id Int32) engine=MergeTree() order by id;"), .{}); // This should fail

    std.debug.print("{d}\n", .{result.affectedRows()});

    result = try conn.exec(@constCast("INSERT INTO test values ({i}),({i}),({i})"), .{ 1, 2, 3 }); // This should fail

    std.debug.print("{d}\n", .{result.affectedRows()});

    std.debug.print("{}\n", .{conn});
    var buffer: [100:0]u8 = undefined; // Sentinel 0 ensures null termination
    const slice = try std.fmt.bufPrint(&buffer, "select * from test", .{});
    const res = try conn.query(slice, .{});
    const rows = try res.toOwnedSlice(alloc, TestStruct);
    defer alloc.free(rows);
    while (res.next()) |row| {
        const s = try row.toOwned(alloc, TestStruct);
        std.debug.print("{}\n", .{s});

        std.debug.print("{}\n", .{row._row});
        const columns = row.columns();
        for (columns) |column| {
            std.debug.print("{s}\n", .{column});
        }
        const row1 = res.rowAt(1);
        const val3: ?i64 = row1.?.get(i64, "id");
        std.debug.print("row at position 1: {d}\n", .{val3.?});
        const val2: ?i64 = row.get(i64, "id");
        std.debug.print("{d}\n", .{val2.?});
    }
}
