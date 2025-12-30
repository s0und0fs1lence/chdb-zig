const std = @import("std");
const Allocator = std.mem.Allocator;

const chdb_headers = @cImport({
    @cInclude("chdb.h");
});

pub const ChdbError = error{
    AllocatorOutOfMemory,
    ConnectionFailed,
    QueryFailed,
    InvalidResult,
    // Add more error cases as needed
};

pub const ChdbConnection = struct {
    conn: [*c][*c]chdb_headers.struct_chdb_connection_,
    allocator: Allocator,
    pub fn init(allocator: Allocator, connectionString: [][]u8) ChdbError!*ChdbConnection {
        var instance = allocator.create(ChdbConnection) catch return ChdbError.AllocatorOutOfMemory;
        instance.allocator = allocator;
        const argc: c_int = @intCast(connectionString.len);
        var argv_slice = allocator.alloc([*c]u8, connectionString.len) catch {
            allocator.destroy(instance);
            return ChdbError.AllocatorOutOfMemory;
        };
        defer allocator.free(argv_slice); // Free this list of pointers after the call
        if (connectionString.len > 0) {
            for (connectionString, 0..) |arg, i| {
                argv_slice[i] = @ptrCast(arg.ptr);
            }
        }

        const conn = chdb_headers.chdb_connect(argc, argv_slice.ptr);
        if (conn == null) {
            allocator.destroy(instance);
            return ChdbError.ConnectionFailed;
        }
        instance.conn = conn;

        return instance;
    }

    pub fn deinit(self: *ChdbConnection) void {
        if (self.conn != null) {
            chdb_headers.chdb_close_conn(self.conn);
            self.conn = null;
        }
        self.allocator.destroy(self);
    }

    pub fn query(self: *ChdbConnection, sql: []u8, format: []u8) ChdbError!ChdbResult {
        const c_sql = self.allocator.dupeZ(u8, sql) catch return ChdbError.AllocatorOutOfMemory;
        defer self.allocator.free(c_sql);
        const c_format = self.allocator.dupeZ(u8, format) catch {
            self.allocator.free(c_sql);
            return ChdbError.AllocatorOutOfMemory;
        };
        defer self.allocator.free(c_format);
        const result = chdb_headers.chdb_query(self.conn.*, c_sql.ptr, c_format.ptr);
        if (result == null) {
            return ChdbError.QueryFailed;
        }
        return ChdbResult{ .res = result };
    }

    pub fn queryStreaming(self: *ChdbConnection, sql: []u8, format: []u8) ChdbError!ChdbResult {
        const c_sql = self.allocator.dupeZ(u8, sql) catch return ChdbError.AllocatorOutOfMemory;
        defer self.allocator.free(c_sql);
        const c_format = self.allocator.dupeZ(u8, format) catch {
            self.allocator.free(c_sql);
            return ChdbError.AllocatorOutOfMemory;
        };
        defer self.allocator.free(c_format);
        const result = chdb_headers.chdb_stream_query(self.conn.*, c_sql.ptr, c_format.ptr);
        if (result == null) {
            return ChdbError.QueryFailed;
        }
        return ChdbResult{ .res = result, ._isStreaming = true };
    }

    pub fn nextStreamingChunk(self: *ChdbConnection, result: *ChdbResult) ChdbError!ChdbResult {
        if (!result.isStreaming()) {
            return ChdbError.InvalidResult;
        }
        const res = chdb_headers.chdb_stream_fetch_result(self.conn.*, result.res);
        if (res != 0) {
            return ChdbError.QueryFailed;
        }
        return ChdbResult{ .res = res, ._isStreaming = true };
    }

    pub fn closeStreaming(self: *ChdbConnection, result: *ChdbResult) void {
        if (result.isStreaming()) {
            chdb_headers.chdb_stream_cancel_query(self.conn.*, result.res);
        }
    }
};

pub const ChdbResult = struct {
    res: [*c]chdb_headers.struct_chdb_result_,
    _isStreaming: bool = false,

    pub fn deinit(self: *ChdbResult) void {
        if (self.res != null) {
            chdb_headers.chdb_destroy_query_result(self.res);
            self.res = null;
        }
    }

    pub fn data(self: *ChdbResult) []u8 {
        if (self.res == null) {
            return &[_]u8{};
        }
        const buf = chdb_headers.chdb_result_buffer(self.res);
        if (buf == null) {
            return &[_]u8{};
        }
        const len = chdb_headers.chdb_result_length(self.res);
        return buf[0..len];
    }

    pub fn isStreaming(self: *ChdbResult) bool {
        return self._isStreaming;
    }

    pub fn size(self: *ChdbResult) usize {
        if (self.res == null) {
            return 0;
        }
        return chdb_headers.chdb_result_length(self.res);
    }

    pub fn elapsedTime(self: *ChdbResult) f64 {
        if (self.res == null) {
            return 0.0;
        }
        return chdb_headers.chdb_result_elapsed(self.res);
    }

    pub fn rowsRead(self: *ChdbResult) u64 {
        if (self.res == null) {
            return 0;
        }
        return chdb_headers.chdb_result_rows_read(self.res);
    }

    pub fn bytesRead(self: *ChdbResult) u64 {
        if (self.res == null) {
            return 0;
        }
        return chdb_headers.chdb_result_bytes_read(self.res);
    }

    pub fn storageRowsRead(self: *ChdbResult) u64 {
        if (self.res == null) {
            return 0;
        }
        return chdb_headers.chdb_result_storage_rows_read(self.res);
    }

    pub fn storageBytesRead(self: *ChdbResult) u64 {
        if (self.res == null) {
            return 0;
        }
        return chdb_headers.chdb_result_storage_bytes_read(self.res);
    }

    pub fn getError(self: *ChdbResult) ?[]u8 {
        if (self.res == null) {
            return null;
        }
        const err = chdb_headers.chdb_result_error(self.res);
        if (err == null) {
            return null;
        }
        return std.mem.span(err);
    }

    pub fn isSuccess(self: *ChdbResult) bool {
        if (self.res == null) {
            return false;
        }
        return self.getError() == null;
    }
};
