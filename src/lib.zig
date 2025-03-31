const std = @import("std");
const types = @import("types.zig"); // Import the Row struct from types.zig
const Row = types.Row; // Import the Row struct from types.zig
const ChSingleRow = types.ChSingleRow; // Import the Row struct from types.zig
const JsonLineIterator = @import("json_iterator.zig").JsonLineIterator;
const ChError = types.ChError;

const chdb = types.chdb;

pub const ChQueryResult = struct {
    res: [*c]chdb.local_result_v2,
    alloc: std.mem.Allocator,
    iter: JsonLineIterator,
    curRow: ?*Row,
    fn init(r: [*c]chdb.local_result_v2, alloc: std.mem.Allocator) !*ChQueryResult {
        var instance = try alloc.create(ChQueryResult);
        instance.res = r;
        instance.alloc = alloc;
        instance.iter = JsonLineIterator.init(std.mem.span(instance.res.*.buf), instance.res.*.rows_read, instance.alloc);
        instance.curRow = null;
        return instance;
    }
    pub fn next(self: *ChQueryResult) ?*Row {
        // the next function is used to get the next row from the iterator
        // and return it as a Row object
        // if the iterator is at the end, return null
        // if we hold a current row, free it
        if (self.curRow) |current| {
            current.deinit();
            self.alloc.destroy(current);
            self.curRow = null;
        }
        self.curRow = self.iter.next();
        return self.curRow;
    }

    pub fn count(self: *ChQueryResult) u64 {
        return self.res.*.rows_read;
    }

    pub fn getIndex(self: *ChQueryResult) usize {
        return self.iter.getIndex();
    }
    pub fn setIndex(self: *ChQueryResult, index: usize) !void {
        return self.iter.setIndex(index);
    }

    pub fn rowAt(self: *ChQueryResult, index: usize) ?*Row {
        // set the position of the iterator to the specified index
        // and return the row at that position
        const curIndex = self.iter.lines.index;
        defer self.iter.lines.index = curIndex;
        self.iter.setIndex(index) catch {
            return null;
        };
        // get the row at the current position
        const row = self.iter.next();
        if (row) |r| {
            // set the iterator back to the original position
            self.iter.lines.index = curIndex;
            return r;
        }

        return null;
    }

    pub fn freeCurrentRow(self: *ChQueryResult) void {
        if (self.curRow) |current| {
            current.deinit();
            self.alloc.destroy(current);
            self.curRow = null;
        }
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

        const clickhouseStr = try std.fmt.allocPrintZ(alloc, comptime "{s}", .{"clickhouse"});
        defer alloc.free(clickhouseStr);
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
        // discard for the moment;
        _ = values;
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
