const std = @import("std");
const chdb_zig = @import("chdb_zig");

const tRes = struct {
    url: []u8,
    tot: u64,
};

fn predicate(it: tRes) bool {
    return it.tot > 15000;
}

pub fn main() !void {
    var x: i32 = 1;
    x += 1;
    const a = "Hello, World!";
    // Prints to stderr, ignoring potential errors.
    std.debug.print("All your {s} are belong to us.\n", .{a});

    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .{};
    const allocator = gpa.allocator();
    const options = chdb_zig.ConnectionOptions{
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
    try cHandle.execute(@constCast(query));

    var result2 = try cHandle.query(@constCast("SELECT URL as url, COUNT(*) as tot FROM my_parquet_table group by url order by tot desc LIMIT 10"));
    if (!result2.isSuccess()) {
        std.debug.print("Query failed: {?s}\n", .{result2.getError()});
        @panic("ERROR");
    }
    var iter = result2.iter(allocator);

    const slice = try iter.selectAsOwned(tRes, predicate);
    std.debug.print("Elapsed time: {d}\n", .{result2.elapsedTime()});
    std.debug.print("Storage Rows Read: {d}\n", .{result2.storageRowsRead()});
    std.debug.print("Rows read: {d}\n", .{result2.rowsRead()});
    std.debug.print("Bytes read: {d}\n", .{result2.bytesRead()});
    std.debug.print("Storage Bytes read: {d}\n", .{result2.storageBytesRead()});
    for (slice.data.slice) |row| {
        std.debug.print("Got a row! URL: {s} -> tot: {d}\n", .{ row.url, row.tot });
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
