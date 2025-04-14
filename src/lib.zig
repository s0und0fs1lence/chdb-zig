const std = @import("std");
const types = @import("types.zig"); // Import the Row struct from types.zig
const Row = types.Row; // Import the Row struct from types.zig
const ChSingleRow = types.ChSingleRow; // Import the Row struct from types.zig

const ChError = types.ChError;
const chdb = types.chdb;
const ChQueryResult = types.ChQueryResult;

const sql_interpolator = @import("sql_interpolator.zig");

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
        // Use TabSeparatedWithNames format for DDL and other non-JSON responses
        instance.format = try std.fmt.allocPrintZ(alloc, comptime "{s}", .{"JsonEachRow"});

        // For in-memory database, we just need to pass program name and path
        var arr = std.ArrayList([*c]u8).init(alloc);
        defer arr.deinit();

        // First argument is program name
        const progname = try std.fmt.allocPrintZ(alloc, comptime "{s}", .{"clickhouse"});
        defer alloc.free(progname);
        try arr.append(progname);

        // Second argument is --path
        const path_arg = try std.fmt.allocPrintZ(alloc, comptime "{s}", .{"--path"});
        defer alloc.free(path_arg);
        try arr.append(path_arg);

        // Third argument is the actual path (":memory:" or provided path)
        const db_path = if (connStr.len == 0) ":memory:" else connStr;
        const path_value = try std.fmt.allocPrintZ(alloc, comptime "{s}", .{db_path});
        defer alloc.free(path_value);
        try arr.append(path_value);

        // Convert to argc/argv format
        const conn = chdb.connect_chdb(@intCast(arr.items.len), @ptrCast(arr.items.ptr));

        if (conn == null) {
            return error.ConnectionFailed;
        }
        instance.conn = conn;

        // For tests, clean up any existing test database state
        if (std.mem.eql(u8, db_path, ":memory:")) {
            var cleanup_result = chdb.query_conn(instance.conn.*, "DROP DATABASE IF EXISTS default", instance.format);
            if (cleanup_result != null) {
                chdb.free_result_v2(cleanup_result);
            }
            cleanup_result = chdb.query_conn(instance.conn.*, "CREATE DATABASE IF NOT EXISTS default", instance.format);
            if (cleanup_result != null) {
                chdb.free_result_v2(cleanup_result);
            }
        }

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
        const full_query = try sql_interpolator.interpolate(self.alloc, q, values);
        defer self.alloc.free(full_query);
        const q_ptr = try std.fmt.allocPrintZ(self.alloc, comptime "{s}", .{full_query});
        defer self.alloc.free(q_ptr);

        if (self.conn == null) return error.ConnectionFailed;
        const res = chdb.query_conn(self.conn.*, q_ptr, "JSONEachRow"); // Use JSON for data queries
        if (res == null) return error.SqlError;

        if (res.*.error_message != null) {
            const msg = std.mem.span(res.*.error_message);
            std.debug.print("SQL Error: {s}\n", .{msg});
            chdb.free_result_v2(res);
            return error.SqlError;
        }

        return ChQueryResult.init(res, self.alloc);
    }

    pub fn exec(self: *ChConn, q: []u8, values: anytype) !ChSingleRow {
        const full_query = try sql_interpolator.interpolate(self.alloc, q, values);
        defer self.alloc.free(full_query);
        const q_ptr = try std.fmt.allocPrintZ(self.alloc, comptime "{s}", .{full_query});
        defer self.alloc.free(q_ptr);

        if (self.conn == null) return error.ConnectionFailed;
        const res = chdb.query_conn(self.conn.*, q_ptr, self.format);
        if (res == null) return error.SqlError;

        if (res.*.error_message != null) {
            const msg = std.mem.span(res.*.error_message);
            std.debug.print("SQL Error: {s}\n", .{msg});
            chdb.free_result_v2(res);
            return error.SqlError;
        }

        const result = ChSingleRow.init(res);
        return result;
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
