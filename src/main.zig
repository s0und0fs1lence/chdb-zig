//! By convention, main.zig is where your main function lives in the case that
//! you are building an executable. If you are making a library, the convention
//! is to delete this file and start with root.zig instead.
const libChdb = @import("lib.zig");
const std = @import("std");
const sql_interpolator = @import("sql_interpolator.zig");

pub fn main() !void {
    // Prints to stderr (it's a shortcut based on `std.io.getStdErr()`)
    std.debug.print("All your {s} are belong to us.\n", .{"codebase"});
    // stdout is for the actual output of your application, for example if you
    // are implementing gzip, then only the compressed bytes should be sent to
    // stdout, not any debugging messages.
    const allocator = std.heap.page_allocator; // Or another allocator
    // const numbers = [_]i32{ 1, 2, 3 };
    const date = "2025-04-07";
    // const userId: u32 = 123;
    // const userName = "O'Malley";
    // const threshold = 99.5;
    const sql_fmt = "SELECT * FROM table WHERE d = {d}";

    // const sql_template = "SELECT event_id FROM events WHERE user_id = {u} AND user_name = {s} AND probability > {f} LIMIT 10";

    const final_sql = try sql_interpolator.interpolate(allocator, sql_fmt, .{date});
    defer allocator.free(final_sql);

    std.debug.print("Generated SQL:\n{s}\n", .{final_sql});
    const alloc = std.heap.smp_allocator;

    const conn = libChdb.ChConn.init(alloc, "--path=/tmp/chdb&readonly=1") catch |err| {
        std.debug.print("Error: {}\n", .{err});
        return err;
    };

    defer conn.deinit();

    var result = try conn.exec(@constCast("CREATE TABLE test (id Int32) engine=MergeTree() order by id;"), .{}); // This should fail

    std.debug.print("{d}\n", .{result.affectedRows()});

    result = try conn.exec(@constCast("INSERT INTO test values (1),(2),(3)"), .{}); // This should fail

    std.debug.print("{d}\n", .{result.affectedRows()});

    std.debug.print("{}\n", .{conn});
    var buffer: [100:0]u8 = undefined; // Sentinel 0 ensures null termination
    const slice = try std.fmt.bufPrint(&buffer, "select * from test", .{});
    const res = try conn.query(slice, .{9000});

    while (res.next()) |row| {
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
