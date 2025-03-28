const std = @import("std");
const chdb = @cImport({
    @cInclude("/workspaces/chdb-zig/header/chdb.h");
});

pub const ChError = error{
    NotValid,
    NotFound,
    TypeMismatch,
};

pub const JsonLineIterator = struct {
    buffer: []const u8, // The entire JSON buffer to be processed
    lines: std.mem.SplitIterator(u8, std.mem.DelimiterType.scalar), // An iterator that yields each line from the buffer
    allocator: std.mem.Allocator, // An allocator to be used for parsing JSON within each line
    pub fn init(buffer: []const u8, allocator: std.mem.Allocator) JsonLineIterator {
        return .{
            .buffer = buffer,
            .lines = std.mem.splitScalar(u8, buffer, '\n'),
            .allocator = allocator,
        };
    }

    pub fn next(self: *JsonLineIterator) ?*Row {
        if (self.lines.next()) |line| {
            const parsed_value = std.json.parseFromSlice(std.json.Value, self.allocator, line, .{}) catch {
                return null;
            };
            // Attempt to parse the current line as a JSON object using std.json.parseFromSlice
            const r = self.allocator.create(Row) catch {
                return null;
            };
            r.value = parsed_value;
            return r;
        }
        return null; // Signal the end of the buffer
    }
};

pub const Row = struct {
    value: std.json.Parsed(std.json.Value), // Holds the parsed JSON value for the current line

    fn deinit(self: *Row) void {
        self.value.deinit();
    }
    pub fn columns(self: *Row) [][]const u8 {
        // TODO: clone the keys
        return self.value.value.object.keys();
    }
    pub fn get(self: *Row, comptime T: type, name: []const u8) ?T {
        const json_value = self.value.value.object.get(name) orelse {
            return null;
        };
        return switch (T) {
            u8 => @as(T, json_value.string),
            i8 => @as(T, @intCast(json_value.integer)),
            i16 => @as(T, @intCast(json_value.integer)),
            i32 => @as(T, @intCast(json_value.integer)),
            i64 => @as(T, @intCast(json_value.integer)),
            u16 => @as(T, @bitCast(json_value.integer)),
            u32 => @as(T, @bitCast(json_value.integer)),
            u64 => @as(T, @bitCast(json_value.integer)),
            f32 => @as(T, @floatCast(json_value.float)),
            f64 => @as(T, json_value.float),
            []u8 => @as(T, @constCast(json_value.string)),
            []const u8 => json_value.string,
            bool => @as(T, json_value.boolean),
            else => null,
        };
    }
};

pub const ChQueryResult = struct {
    res: [*c]chdb.local_result_v2,
    alloc: std.mem.Allocator,
    iter: JsonLineIterator,
    curRow: ?*Row,
    fn init(r: [*c]chdb.local_result_v2, alloc: std.mem.Allocator) !*ChQueryResult {
        var instance = try alloc.create(ChQueryResult);
        instance.res = r;
        instance.alloc = alloc;
        instance.iter = JsonLineIterator.init(std.mem.span(instance.res.*.buf), instance.alloc);
        instance.curRow = null;
        return instance;
    }
    pub fn next(self: *ChQueryResult) ?*Row {
        if (self.curRow) |current| {
            current.deinit();
        }
        self.curRow = self.iter.next();
        return self.curRow;
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

    pub fn init(alloc: std.mem.Allocator, connStr: []const u8) !*ChConn {
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

        if (conn == null) {
            std.debug.print("Connection failed\n", .{});
            return error.NotValid;
        }
        instance.conn = conn;

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
                return try ChQueryResult.init(res, self.alloc);
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
