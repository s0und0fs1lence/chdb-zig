const std = @import("std");
const chdb = @cImport({
    @cInclude("/workspaces/chdb-zig/header/chdb.h");
});

pub const ChError = error{
    NotValid,
};

pub const ChQueryResult = struct {
    res: [*c]chdb.local_result_v2,
    alloc: std.mem.Allocator,
    fn new(r: [*c]chdb.local_result_v2, alloc: std.mem.Allocator) !*ChQueryResult {
        var instance = try alloc.create(ChQueryResult);
        instance.res = r;
        instance.alloc = alloc;
        return instance;
    }
    pub fn next(self: *ChQueryResult) void {
        if (self.res.*.rows_read > 0) {}
    }
    pub fn free(self: *ChQueryResult) void {
        if (self.res != null) {
            chdb.free_result_v2(self.res);
        }
        self.alloc.destroy(self);
    }
};

pub const ChConn = struct {
    conn: [*c][*c]chdb.chdb_conn,
    alloc: std.mem.Allocator,

    pub fn new(alloc: std.mem.Allocator, connStr: []const u8) !*ChConn {
        var instance = try alloc.create(ChConn);
        instance.alloc = alloc;
        var tokenizer = std.mem.tokenizeAny(u8, connStr, "&");
        var arr = std.ArrayList([*c]u8).init(alloc);
        const clickhouseStr = try std.fmt.allocPrintZ(instance.alloc, comptime "{s}", .{"clickhouse"});
        defer instance.alloc.free(clickhouseStr);
        try arr.append(clickhouseStr);
        while (tokenizer.next()) |tok| {
            if (tok.len == 0) {
                @branchHint(.unlikely);
                continue;
            }
            const str_ptr = try std.fmt.allocPrintZ(instance.alloc, comptime "{s}", .{tok});
            defer instance.alloc.free(str_ptr);
            try arr.append(str_ptr);
        }

        // Convert to C-compatible format
        var argv = try instance.alloc.alloc([*c]u8, arr.items.len);
        defer instance.alloc.free(argv);

        for (arr.items, 0..) |arg, i| {
            argv[i] = arg; // Convert to C-string pointer
        }
        const conn = chdb.connect_chdb(@intCast(argv.len), @ptrCast(argv.ptr));

        instance.conn = conn;
        if (conn == null) {
            std.debug.print("Connection failed\n", .{});
            return instance;
        }

        return instance;
    }

    pub fn query(self: *ChConn, q: []u8, format: []u8) !*ChQueryResult {
        const q_ptr = try std.fmt.allocPrintZ(self.alloc, comptime "{s}", .{q});
        defer self.alloc.free(q_ptr);
        const f_ptr = try std.fmt.allocPrintZ(self.alloc, comptime "{s}", .{format});
        defer self.alloc.free(f_ptr);
        const res = chdb.query_conn(self.conn.*, q_ptr, f_ptr);
        if (res != null) {
            const result_struct = res.*; // Unwrap the optional pointer

            if (result_struct.error_message == null) {
                return try ChQueryResult.new(res, self.alloc);
            } else {
                chdb.free_result_v2(res);
                return error.NotValid;
            }
        }
        return ChError.NotValid;
    }

    pub fn deinit(self: *ChConn) void {
        // clean references
        // close connection if still avaiable
        if (self.conn != null) {
            chdb.close_conn(self.conn);
        }

        self.alloc.destroy(self);
    }
};
