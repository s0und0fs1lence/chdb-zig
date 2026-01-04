const std = @import("std");
const chdb_zig = @import("chdb_zig");

pub fn main() !void {
    var x: i32 = 1;
    x += 1;
    const a = "Hello, World!";
    // Prints to stderr, ignoring potential errors.
    std.debug.print("All your {s} are belong to us.\n", .{a});

    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .{};
    const allocator = gpa.allocator();
    const options = chdb_zig.ChdbConnectionOptions{
        .UseMultiQuery = true,
        .Path = "/workspaces/chdb-zig/test.db",
    };

    const cHandle = try chdb_zig.initConnection(allocator, options);
    defer cHandle.deinit();

    const query =
        \\CREATE TABLE IF NOT EXISTS my_parquet_table ENGINE = MergeTree() 
        \\ ORDER BY tuple() -- Adjust the ordering key as needed for performance
        \\ AS SELECT * FROM url('https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet');
    ;
    var result = try cHandle.query(@constCast(query));
    if (!result.isSuccess()) {
        std.debug.print("Query failed: {?s}\n", .{result.getError()});
        @panic("ERROR");
    }
    std.debug.print("Insert Elapsed time: {d}\n", .{result.elapsedTime()});
    std.debug.print("Insert Rows read: {d}\n", .{result.rowsRead()});
    std.debug.print("Insert storage Rows read: {d}\n", .{result.storageRowsRead()});
    std.debug.print("Insert Bytes read: {d}\n", .{result.bytesRead()});
    std.debug.print("Insert storage Bytes read: {d}\n", .{result.storageBytesRead()});

    var result2 = try cHandle.query(@constCast("SELECT URL, COUNT(*) FROM my_parquet_table group by URL order by COUNT(*) desc LIMIT 10"));
    if (!result2.isSuccess()) {
        std.debug.print("Query failed: {?s}\n", .{result2.getError()});
        @panic("ERROR");
    }
    var iter = result2.iter(allocator);

    const slice = iter.nextRow();
    if (slice) |s| {
        std.debug.print("Got a row! {s}\n", .{s});
        std.debug.print("Elapsed time: {d}\n", .{result2.elapsedTime()});
        std.debug.print("Storage Rows Read: {d}\n", .{result2.storageRowsRead()});
        std.debug.print("Rows read: {d}\n", .{result2.rowsRead()});
        std.debug.print("Bytes read: {d}\n", .{result2.bytesRead()});
        std.debug.print("Storage Bytes read: {d}\n", .{result2.storageBytesRead()});
    } else {
        std.debug.print("No rows found.\n", .{});
    }
    // defer slice.deinit();
    // switch (slice.data) {
    //     .single => |_| @panic("could not be possible"),
    //     .slice => |users| {
    //         std.debug.print("Found {} users\n", .{users.len});
    //         for (users) |u| std.debug.print("Number: {s}\n", .{u});
    //     },
    // }
}

test "basic connection initialization" {
    const allocator = std.testing.allocator;
    const options = chdb_zig.ChdbConnectionOptions{ .UseMultiQuery = true };
    const cHandle = try chdb_zig.ChdbConnection.init(allocator, options);
    try std.testing.expect(cHandle.conn != null);
}
