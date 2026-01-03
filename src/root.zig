//! By convention, root.zig is the root source file when making a library.
const std = @import("std");
const chdb_zig = @import("chdb.zig");

pub const ChdbConnection = chdb_zig.ChdbConnection;
pub const ChdbError = chdb_zig.ChdbError;
pub const ChdbResult = chdb_zig.ChdbResult;
pub const ChdbStreamingHandle = chdb_zig.ChdbStreamingHandle;
pub const ChdbIterator = chdb_zig.ChdbIterator;

pub const initConnection = ChdbConnection.init;
