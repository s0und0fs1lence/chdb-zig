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
    InvalidRow,
    // Add more error cases as needed
};

pub const ChdbConnectionOptions = struct {
    // Future options can be added here
    UseMultiQuery: bool = false,
    Path: ?[]const u8 = null,
    ConfigFilePath: ?[]const u8 = null,
    LogLevel: ?[]const u8 = null,
    /// Custom argument.
    ///
    /// "--param=value" is the format accepted
    /// We should tell user where to look for officially supported arguments.
    /// Here is some hint for now: <https://github.com/fixcik/chdb-rs/blob/master/OPTIONS.md>.
    CustomArgs: ?[]const []const u8 = null,
};

pub const ChdbConnection = struct {
    conn: [*c][*c]chdb_headers.struct_chdb_connection_,
    allocator: Allocator,
    // At the moment, only JSONEachRow is supported
    defaultFormat: [:0]u8,
    pub fn init(allocator: Allocator, options: ChdbConnectionOptions) ChdbError!*ChdbConnection {
        var instance = allocator.create(ChdbConnection) catch return ChdbError.AllocatorOutOfMemory;
        instance.allocator = allocator;
        errdefer allocator.destroy(instance);
        // Track duplicated strings so we can free them later
        var allocated_strings: std.ArrayList([:0]u8) = .{};
        var lst: std.ArrayList([*c]u8) = .{};
        defer {
            for (allocated_strings.items) |s| allocator.free(s);
            allocated_strings.deinit(allocator);
            lst.deinit(allocator);
        }

        lst.append(allocator, @constCast("chdb")) catch return ChdbError.AllocatorOutOfMemory;

        if (options.Path) |path| {
            const duped: [:0]u8 = std.fmt.allocPrintSentinel(allocator, "--path={s}", .{path}, 0) catch return ChdbError.AllocatorOutOfMemory;
            allocated_strings.append(allocator, duped) catch return ChdbError.AllocatorOutOfMemory; // Save for cleanup later
            lst.append(allocator, @constCast(duped)) catch return ChdbError.AllocatorOutOfMemory;
        }
        if (options.UseMultiQuery) {
            lst.append(allocator, @constCast("--multiquery")) catch return ChdbError.AllocatorOutOfMemory;
        }
        if (options.LogLevel) |level| {
            const duped: [:0]u8 = std.fmt.allocPrintSentinel(allocator, "--log-level={s}", .{level}, 0) catch return ChdbError.AllocatorOutOfMemory;

            allocated_strings.append(allocator, duped) catch return ChdbError.AllocatorOutOfMemory; // Save for cleanup later

            lst.append(allocator, @constCast(duped)) catch return ChdbError.AllocatorOutOfMemory;
        }

        if (options.CustomArgs) |customArgs| {
            for (customArgs) |arg| {
                const duped = allocator.dupeZ(u8, arg) catch return ChdbError.AllocatorOutOfMemory;
                allocated_strings.append(allocator, duped) catch return ChdbError.AllocatorOutOfMemory; // Save for cleanup later

                lst.append(allocator, @constCast(duped)) catch return ChdbError.AllocatorOutOfMemory;
            }
        }

        // Now when we call this, duped_path is still valid memory
        const conn = chdb_headers.chdb_connect(@intCast(lst.items.len), lst.items.ptr);
        if (conn == null) {
            allocator.destroy(instance);
            return ChdbError.ConnectionFailed;
        }
        instance.conn = conn;
        instance.defaultFormat = allocator.dupeZ(u8, "JSONEachRow") catch return ChdbError.AllocatorOutOfMemory;

        return instance;
    }

    // Currently only JSONEachRow is supported, but this is a placeholder for future formats
    pub fn setDefaultFormat(self: *ChdbConnection, format: []u8) void {
        _ = self;
        _ = format;
    }

    pub fn deinit(self: *ChdbConnection) void {
        if (self.conn != null) {
            chdb_headers.chdb_close_conn(self.conn);
            self.conn = null;
        }
        self.allocator.destroy(self);
    }

    pub fn query(self: *ChdbConnection, sql: []u8) ChdbError!ChdbResult {
        const c_sql = self.allocator.dupeZ(u8, sql) catch return ChdbError.AllocatorOutOfMemory;
        defer self.allocator.free(c_sql);

        const result = chdb_headers.chdb_query(self.conn.*, c_sql.ptr, self.defaultFormat.ptr);
        if (result == null) {
            return ChdbError.QueryFailed;
        }
        return ChdbResult{ .res = result };
    }

    pub fn queryStreaming(self: *ChdbConnection, sql: []u8) ChdbError!ChdbStreamingHandle {
        const c_sql = self.allocator.dupeZ(u8, sql) catch return ChdbError.AllocatorOutOfMemory;
        defer self.allocator.free(c_sql);

        const result = chdb_headers.chdb_stream_query(self.conn.*, c_sql.ptr, self.defaultFormat.ptr);
        if (result == null) {
            return ChdbError.QueryFailed;
        }
        return ChdbStreamingHandle{ .handle = result };
    }

    pub fn nextStreamingChunk(self: *ChdbConnection, result: *ChdbStreamingHandle) ChdbError!ChdbResult {
        const res = chdb_headers.chdb_stream_fetch_result(self.conn.*, result.handle);
        if (res == null) {
            return ChdbError.QueryFailed;
        }
        return ChdbResult{ .res = res };
    }

    pub fn closeStreaming(self: *ChdbConnection, result: *ChdbStreamingHandle) void {
        if (result.handle != null) {
            chdb_headers.chdb_stream_cancel_query(self.conn.*, result.handle);
        }
    }
};

pub const ChdbStreamingHandle = struct {
    handle: [*c]chdb_headers.struct_chdb_result_,

    pub fn deinit(self: *ChdbStreamingHandle) void {
        if (self.handle != null) {
            chdb_headers.chdb_destroy_query_result(self.handle);
            self.handle = null;
        }
    }

    pub fn getError(self: *ChdbStreamingHandle) ?[]const u8 {
        if (self.handle == null) {
            return null;
        }
        const err = chdb_headers.chdb_result_error(self.handle);
        if (err == null) {
            return null;
        }
        return std.mem.span(err);
    }

    pub fn isSuccess(self: *ChdbStreamingHandle) bool {
        if (self.handle == null) {
            return false;
        }
        return self.getError() == null;
    }
};

pub const ChdbResult = struct {
    res: [*c]chdb_headers.struct_chdb_result_,

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

    /// Returns an iterator over the result rows (NDJSON)
    /// Allocator is needed for any allocations during iteration
    /// The iterator returns rows as slices of the original buffer (zero-copy)
    /// But it can allocate a json parser if needed
    /// Pass an arena allocator for best performance, otherwise free each row manually
    pub fn iter(self: *ChdbResult, arena: Allocator) ChdbIterator {
        return ChdbIterator.init(self, arena);
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

    pub fn getError(self: *ChdbResult) ?[]const u8 {
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

/// The Data itself (The Union)
pub fn ChdbData(comptime T: type) type {
    return union(enum) {
        single: T,
        slice: []T,
    };
}

/// The Result Wrapper (The Struct)
pub fn ChdbResultValue(comptime T: type) type {
    return struct {
        data: ChdbData(T),
        _arena: *std.heap.ArenaAllocator,

        pub fn deinit(self: @This()) void {
            const child_allocator = self._arena.child_allocator;
            // Frees all strings inside T (single or slice)
            self._arena.deinit();
            // Frees the arena object itself
            child_allocator.destroy(self._arena);
        }

        pub fn isSingle(self: @This()) bool {
            return switch (self.data) {
                .single => true,
                .slice => false,
            };
        }
    };
}

//NDJSON iterator for ChdbResult
pub const ChdbIterator = struct {
    // We store the raw buffer and use an iterator that doesn't allocate
    iter: std.mem.SplitIterator(u8, .scalar),
    _rowCount: usize,
    currentIndex: usize,
    allocator: Allocator,
    dataSizeInBytes: usize,

    pub fn init(result: *ChdbResult, allocator: Allocator) ChdbIterator {
        const data_buf = result.data();
        if (data_buf.len == 0) {
            return ChdbIterator{
                .iter = std.mem.splitScalar(u8, "", '\n'),
                ._rowCount = 0,
                .currentIndex = 0,
                .allocator = allocator,
                .dataSizeInBytes = 0,
            };
        }

        // Count rows once if you need rowCount, otherwise skip this loop
        // to be even faster.
        var count: usize = 0;
        for (data_buf) |byte| {
            if (byte == '\n') count += 1;
        }

        // Handle files that don't end in a trailing newline
        if (data_buf[data_buf.len - 1] != '\n') count += 1;

        return ChdbIterator{
            .iter = std.mem.splitScalar(u8, data_buf, '\n'),
            ._rowCount = count,
            .currentIndex = 0,
            .allocator = allocator,
            .dataSizeInBytes = data_buf.len,
        };
    }

    pub fn maxMemoryUsage(self: *ChdbIterator) usize {
        return self.dataSizeInBytes * 2;
    }

    fn getArenaAllocator(self: *ChdbIterator) ChdbError!*std.heap.ArenaAllocator {
        var arena = self.allocator.create(std.heap.ArenaAllocator) catch return ChdbError.AllocatorOutOfMemory;
        arena.* = std.heap.ArenaAllocator.init(self.allocator);
        errdefer {
            arena.deinit();
            self.allocator.destroy(arena);
        }
        return arena;
    }

    /// Returns the next row as a slice of the original buffer (Zero-copy)
    pub fn nextRow(self: *ChdbIterator) ?[]const u8 {
        while (self.iter.next()) |line| {
            // Chdb sometimes leaves empty lines at the end of NDJSON
            if (line.len == 0) continue;

            self.currentIndex += 1;
            return line;
        }
        return null;
    }

    pub fn nextAs(self: *ChdbIterator, comptime T: type) ChdbError!?ChdbResultValue(T) {
        const line = self.nextRow() orelse return null;

        const arena = try self.getArenaAllocator();

        const parsed = try std.json.parseFromSlice(T, arena.allocator(), line, .{
            .ignore_unknown_fields = true,
        });

        return .{
            .data = .{ .single = parsed.value },
            ._arena = arena,
        };
    }

    pub fn reset(self: *ChdbIterator) void {
        self.iter.reset();
        self.currentIndex = 0;
    }

    pub fn rowAt(self: *ChdbIterator, index: usize) ?[]const u8 {
        self.reset();
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            if (self.currentIndex == index) {
                return line;
            }
            self.currentIndex += 1;
        }
        return null;
    }

    pub fn sliceOwned(self: *ChdbIterator, start: usize, end: usize) ChdbError!ChdbResultValue([]const []const u8) {
        var rows: std.ArrayList([]const u8) = .{};
        defer rows.deinit(self.allocator);
        self.reset();
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            if (self.currentIndex >= start and self.currentIndex < end) {
                rows.append(self.allocator, line) catch ChdbError.AllocatorOutOfMemory;
            }
            self.currentIndex += 1;
            if (self.currentIndex >= end) break;
        }
        const res = rows.toOwnedSlice(self.allocator) catch return ChdbError.AllocatorOutOfMemory;
        return res;
    }

    pub fn sliceAsOwned(self: *ChdbIterator, comptime T: type, start: usize, end: usize) ChdbError!ChdbResultValue(T) {
        const arena = try self.getArenaAllocator();
        const arena_allocator = arena.allocator();
        var rows: std.ArrayList(T) = .{};
        self.reset();
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            if (self.currentIndex >= start and self.currentIndex < end) {
                const parsed = std.json.parseFromSlice(T, arena_allocator, line, .{ .ignore_unknown_fields = true }) catch {
                    return ChdbError.InvalidRow;
                };

                rows.append(arena_allocator, parsed.value) catch return ChdbError.AllocatorOutOfMemory;
            }
            self.currentIndex += 1;
            if (self.currentIndex >= end) break;
        }
        const res = rows.toOwnedSlice(arena_allocator) catch return ChdbError.AllocatorOutOfMemory;
        return .{
            .data = .{ .slice = res },
            ._arena = arena,
        };
    }

    pub fn takeOwned(self: *ChdbIterator, count: usize) ChdbError![]const []const u8 {
        var rows: std.ArrayList([]const u8) = .{};
        defer rows.deinit(self.allocator);
        var taken: usize = 0;
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            if (taken < count) {
                rows.append(self.allocator, line) catch return ChdbError.AllocatorOutOfMemory;
                taken += 1;
            } else {
                break;
            }
        }
        const res = rows.toOwnedSlice(self.allocator) catch return ChdbError.AllocatorOutOfMemory;
        return res;
    }

    pub fn takeAsOwned(self: *ChdbIterator, comptime T: type, count: usize) ChdbError![]T {
        var rows = std.ArrayList(T).initCapacity(self.allocator, count) catch return ChdbError.AllocatorOutOfMemory;
        defer rows.deinit(self.allocator);
        var taken: usize = 0;
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            if (taken < count) {
                const parsed = std.json.parseFromSlice(T, self.allocator, line, .{ .ignore_unknown_fields = true }) catch {
                    return ChdbError.InvalidResult;
                };
                defer parsed.deinit();

                rows.appendAssumeCapacity(parsed.value);
                taken += 1;
            } else {
                break;
            }
        }
        const res = rows.toOwnedSlice(self.allocator) catch return ChdbError.AllocatorOutOfMemory;
        return res;
    }

    pub fn selectOwned(self: *ChdbIterator, predicate: fn ([]const u8) bool) ChdbError![]const []const u8 {
        var rows: std.ArrayList([]const u8) = .{};
        defer rows.deinit(self.allocator);
        self.reset();
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            if (predicate(line)) {
                rows.append(self.allocator, line) catch break;
            }
        }
        const res = rows.toOwnedSlice(self.allocator) catch return ChdbError.AllocatorOutOfMemory;
        return res;
    }

    pub fn selectAsOwned(self: *ChdbIterator, comptime T: type, predicate: fn (T) bool) ChdbError![]T {
        var rows: std.ArrayList(T) = .{};
        defer rows.deinit(self.allocator);
        self.reset();
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            const parsed = std.json.parseFromSlice(T, self.allocator, line, .{ .ignore_unknown_fields = true }) catch {
                return ChdbError.InvalidResult;
            };
            defer parsed.deinit();
            if (predicate(parsed.value)) {
                rows.append(self.allocator, parsed.value) catch break;
            }
        }
        const res = rows.toOwnedSlice(self.allocator) catch return ChdbError.AllocatorOutOfMemory;
        return res;
    }

    pub fn rowCount(self: *ChdbIterator) usize {
        return self._rowCount;
    }
};
