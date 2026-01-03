const std = @import("std");
const chdb_zig = @import("chdb_zig");

pub fn main() !void {
    var x: i32 = 1;
    x += 1;
    const a = "Hello, World!";
    // Prints to stderr, ignoring potential errors.
    std.debug.print("All your {s} are belong to us.\n", .{a});

    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .{};
    var allocator = gpa.allocator();
    const array = try allocator.alloc([]u8, 0);
    defer allocator.free(array);
    const cHandle = try chdb_zig.ChdbConnection.init(allocator, array);
    defer cHandle.deinit();
    var result = try cHandle.query(@constCast("select *,'Ass' as t from system.numbers limit 1000;"));
    var iter = result.iter(allocator);
    const Us = struct {
        number: i64,
        t: []u8,
    };

    const slice = try iter.sliceAsOwned(Us, 0, 1000);
    defer slice.deinit();
    switch (slice.data) {
        .single => |_| @panic("could not be possible"),
        .slice => |users| {
            std.debug.print("Found {} users\n", .{users.len});
            for (users) |u| std.debug.print("Number: {d} - T: {s}\n", .{ u.number, u.t });
        },
    }
}

test "basic connection initialization" {
    const allocator = std.testing.allocator;
    const array = try allocator.alloc([]u8, 0);
    defer allocator.free(array);
    const cHandle = try chdb_zig.ChdbConnection.init(allocator, array);
    try std.testing.expect(cHandle.conn != null);
}
