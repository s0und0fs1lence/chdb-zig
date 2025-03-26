//! By convention, root.zig is the root source file when making a library. If
//! you are making an executable, the convention is to delete this file and
//! start with main.zig instead.
const std = @import("std");
const testing = std.testing;

const chdb = @cImport({
    @cInclude("/workspaces/chdb-zig/header/chdb.h");
});
pub export fn add(a: i32, b: i32) i32 {
    return a + b;
}

pub const ChConn = struct {
    conn: chdb.chdb_conn,
    alloc: std.mem.Allocator,
    pub fn new(self: *ChConn, alloc: std.mem.Allocator) *ChConn {
        self.alloc = alloc;
    }
};

test "basic add functionality" {
    try testing.expect(add(3, 7) == 10);
}
