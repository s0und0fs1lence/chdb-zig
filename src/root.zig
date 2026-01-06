//! By convention, root.zig is the root source file when making a library.
const std = @import("std");
const chdb_zig = @import("chdb.zig");

pub const Connection = chdb_zig.Connection;
pub const Error = chdb_zig.Error;
pub const Result = chdb_zig.Result;
pub const StreamingHandle = chdb_zig.StreamingHandle;
pub const Iterator = chdb_zig.Iterator;
pub const ConnectionOptions = chdb_zig.ConnectionOptions;

pub const initConnection = Connection.init;

test "basic connection initialization" {
    const allocator = std.testing.allocator;
    const options = chdb_zig.ConnectionOptions{ .UseMultiQuery = true };
    const cHandle = try chdb_zig.Connection.init(allocator, options);
    defer cHandle.deinit();
    try std.testing.expect(cHandle.conn != null);
}
