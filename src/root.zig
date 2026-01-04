//! By convention, root.zig is the root source file when making a library.
const std = @import("std");
const chdb_zig = @import("chdb.zig");

pub const ChdbConnection = chdb_zig.ChdbConnection;
pub const ChdbError = chdb_zig.ChdbError;
pub const ChdbResult = chdb_zig.ChdbResult;
pub const ChdbStreamingHandle = chdb_zig.ChdbStreamingHandle;
pub const ChdbIterator = chdb_zig.ChdbIterator;
pub const ChdbConnectionOptions = chdb_zig.ChdbConnectionOptions;

pub const initConnection = ChdbConnection.init;

test "basic connection initialization" {
    const allocator = std.testing.allocator;
    const options = chdb_zig.ChdbConnectionOptions{ .UseMultiQuery = true };
    const cHandle = try chdb_zig.ChdbConnection.init(allocator, options);
    defer cHandle.deinit();
    try std.testing.expect(cHandle.conn != null);
}
