//! By convention, root.zig is the root source file when making a library.
const std = @import("std");
const chdb_zig = @import("chdb.zig");

pub fn bufferedPrint() !void {

    // Stdout is for the actual output of your application, for example if you
    // are implementing gzip, then only the compressed bytes should be sent to
    // stdout, not any debugging messages.
    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    try stdout.print("Run `zig build test` to run the tests.\n", .{});

    try stdout.flush(); // Don't forget to flush!
}

pub fn add(a: i32, b: i32) i32 {
    return a + b;
}

pub fn testConnection() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const deinit_status = gpa.deinit();
        //fail test; can't try in defer as defer is executed after we return
        if (deinit_status == .leak) std.testing.expect(false) catch @panic("TEST FAIL");
    }
    var allocator: std.mem.Allocator = gpa.allocator();
    const array = try allocator.alloc([]u8, 0);
    defer allocator.free(array);
    var conn = try chdb_zig.ChdbConnection.init(allocator, array);
    defer conn.deinit();

    var result = try conn.queryStreaming(@constCast("select * from system.numbers limit 100000;"), @constCast("CSV"));
    defer result.deinit();
    var result2 = try conn.nextStreamingChunk(&result);
    defer result2.deinit();
    const rows = result2.rowsRead();
    const bytesRead = result2.bytesRead();
    const data = result2.data();
    std.debug.print("Bytes read: {d}\n", .{bytesRead});
    std.debug.print("Rows read: {d}\n", .{rows});
    std.debug.print("Data size: {s}\n", .{data});
}

test "basic add functionality" {
    try std.testing.expect(add(3, 7) == 10);
}

test "chdbConnect works" {
    const gpa = std.testing.allocator;
    const array: [][]u8 = undefined;
    const conn = try chdb_zig.ChdbConnection.init(gpa, array);
    defer gpa.destroy(conn);
    std.debug.print("Connection established: {any}\n", .{conn.conn});
    try std.testing.expect(conn.conn != null);
}
