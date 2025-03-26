//! By convention, main.zig is where your main function lives in the case that
//! you are building an executable. If you are making a library, the convention
//! is to delete this file and start with root.zig instead.
const c = @import("lib.zig");

pub fn main() !void {
    // Prints to stderr (it's a shortcut based on `std.io.getStdErr()`)
    std.debug.print("All your {s} are belong to us.\n", .{"codebase"});
    // stdout is for the actual output of your application, for example if you
    // are implementing gzip, then only the compressed bytes should be sent to
    // stdout, not any debugging messages.

    const alloc = std.heap.smp_allocator;
    const conn = c.ChConn.new(alloc, "--path=/tmp/chdb&readonly=1") catch |err| {
        std.debug.print("Error: {}\n", .{err});
        return err;
    };

    defer conn.deinit();
    std.debug.print("{*}\n", .{conn});
    var buffer: [100:0]u8 = undefined; // Sentinel 0 ensures null termination
    const slice = try std.fmt.bufPrint(&buffer, "select 1 as a,2 as b", .{});
    const res = try conn.query(slice, @constCast("JSONEachRow"));
    std.debug.print("{*}\n", .{res});
}

test "simple test" {
    var list = std.ArrayList(i32).init(std.testing.allocator);
    defer list.deinit(); // Try commenting this out and see if zig detects the memory leak!
    try list.append(42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}

test "use other module" {
    try std.testing.expectEqual(@as(i32, 150), lib.add(100, 50));
}

test "fuzz example" {
    const Context = struct {
        fn testOne(context: @This(), input: []const u8) anyerror!void {
            _ = context;
            // Try passing `--fuzz` to `zig build test` and see if it manages to fail this test case!
            try std.testing.expect(!std.mem.eql(u8, "canyoufindme", input));
        }
    };
    try std.testing.fuzz(Context{}, Context.testOne, .{});
}

const std = @import("std");

/// This imports the separate module containing `root.zig`. Take a look in `build.zig` for details.
const lib = @import("chdb_zig_lib");
