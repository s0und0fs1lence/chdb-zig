const std = @import("std");
const chdb = @cImport({
    @cInclude("/workspaces/chdb-zig/header/chdb.h");
});

pub const ChError = error{
    ConnectionFailed,
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
    pub fn get(self: *Row, T: type, name: []const u8) ?T {
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

pub const ChSingleRow = struct {
    elapsed: f64,
    rows_read: u64,
    error_message: [*c]u8,
    fn init(res: [*c]chdb.local_result_v2) !ChSingleRow {
        return ChSingleRow{
            .elapsed = res.*.elapsed,
            .rows_read = res.*.rows_read,
            .error_message = res.*.error_message,
        };
    }

    pub fn elapsedSec(self: *ChSingleRow) f64 {
        return self.elapsed;
    }
    pub fn affectedRows(self: *ChSingleRow) u64 {
        return self.rows_read;
    }
    pub fn isError(self: *ChSingleRow) bool {
        return self.error_message != null;
    }
    pub fn errorMessage(self: *ChSingleRow) [*c]u8 {
        return self.error_message;
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
            self.alloc.destroy(current);
            self.curRow = null;
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
    format: [:0]u8,
    /// This function is used to initialize a connection to the ClickHouse database.
    /// It takes an allocator and a connection string as input.
    /// The connection string should be in the format of "key=value&key2=value2".
    /// The function returns a pointer to a ChConn object or an error if the connection fails.
    /// The function uses the allocator to allocate memory for the connection string and
    /// the connection object. Also, it pass this allocator to all the query results retrieved by this connection.
    pub fn init(alloc: std.mem.Allocator, connStr: []const u8) !*ChConn {

        // create a new instance of ChConn
        // and allocate memory for the connection string
        // and the connection object
        var instance = try alloc.create(ChConn);
        instance.alloc = alloc;
        // cast the format to a C-compatible string
        // and allocate memory for it
        instance.format = try std.fmt.allocPrintZ(alloc, comptime "{s}", .{"JSONEachRow"});

        // tokenize the connection string
        // and create an array of C-compatible strings
        // using the allocator
        var tokenizer = std.mem.tokenizeAny(u8, connStr, "&");
        var arr = std.ArrayList([*c]u8).init(alloc);

        const clickhouseStr = "clickhouse\x00";

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
            return ChError.ConnectionFailed;
        }
        instance.conn = conn;

        return instance;
    }

    /// This function is used to execute a query on the ClickHouse database
    /// and return the result as a ChQueryResult object.
    ///
    /// It takes a query string and a format string as input.
    /// The format string specifies the output format of the query result.
    ///
    /// The function returns a ChQueryResult object or an error if the query fails.
    /// The function allocate memory for the query string and format string using the allocator
    /// provided in the ChConn object.
    pub fn query(self: *ChConn, q: []u8, values: anytype) !*ChQueryResult {
        std.debug.print("{}\n", .{values.len});
        const q_ptr = try std.fmt.allocPrintZ(self.alloc, comptime "{s}", .{q});
        defer self.alloc.free(q_ptr);

        const res = chdb.query_conn(self.conn.*, q_ptr, self.format);
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

    pub fn exec(self: *ChConn, q: []u8, values: anytype) !ChSingleRow {
        // discard for the moment;
        _ = values;

        const q_ptr = try std.fmt.allocPrintZ(self.alloc, comptime "{s}", .{q});
        defer self.alloc.free(q_ptr);

        const res = chdb.query_conn(self.conn.*, q_ptr, self.format);
        if (res != null) {
            return ChSingleRow.init(res);
        }
        return ChError.NotValid;
    }

    pub fn deinit(self: *ChConn) void {
        // clean references
        // close connection if still avaiable
        if (self.conn != null) {
            chdb.close_conn(self.conn);
        }
        self.alloc.free(self.format);

        self.alloc.destroy(self);
    }
};
