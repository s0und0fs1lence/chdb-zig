const std = @import("std");
const Allocator = std.mem.Allocator;

const chdb_headers = @cImport({
    @cInclude("chdb.h");
});

pub const Error = error{
    AllocatorOutOfMemory,
    ConnectionFailed,
    QueryFailed,
    InvalidResult,
    InvalidRow,
    // Add more error cases as needed
};

pub const ConnectionOptions = struct {
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

pub const Connection = struct {
    conn: [*c][*c]chdb_headers.struct_chdb_connection_,
    allocator: Allocator,
    // At the moment, only JSONEachRow is supported
    defaultFormat: [:0]u8,
    pub fn init(allocator: Allocator, options: ConnectionOptions) Error!*Connection {
        var instance = allocator.create(Connection) catch return Error.AllocatorOutOfMemory;
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

        lst.append(allocator, @constCast("chdb")) catch return Error.AllocatorOutOfMemory;

        if (options.Path) |path| {
            const duped: [:0]u8 = std.fmt.allocPrintSentinel(allocator, "--path={s}", .{path}, 0) catch return Error.AllocatorOutOfMemory;
            allocated_strings.append(allocator, duped) catch return Error.AllocatorOutOfMemory; // Save for cleanup later
            lst.append(allocator, @constCast(duped)) catch return Error.AllocatorOutOfMemory;
        }
        if (options.UseMultiQuery) {
            lst.append(allocator, @constCast("--multiquery")) catch return Error.AllocatorOutOfMemory;
        }
        if (options.LogLevel) |level| {
            const duped: [:0]u8 = std.fmt.allocPrintSentinel(allocator, "--log-level={s}", .{level}, 0) catch return Error.AllocatorOutOfMemory;

            allocated_strings.append(allocator, duped) catch return Error.AllocatorOutOfMemory; // Save for cleanup later

            lst.append(allocator, @constCast(duped)) catch return Error.AllocatorOutOfMemory;
        }

        if (options.CustomArgs) |customArgs| {
            for (customArgs) |arg| {
                const duped = allocator.dupeZ(u8, arg) catch return Error.AllocatorOutOfMemory;
                allocated_strings.append(allocator, duped) catch return Error.AllocatorOutOfMemory; // Save for cleanup later

                lst.append(allocator, @constCast(duped)) catch return Error.AllocatorOutOfMemory;
            }
        }

        // Now when we call this, duped_path is still valid memory
        const conn = chdb_headers.chdb_connect(@intCast(lst.items.len), lst.items.ptr);
        if (conn == null) {
            return Error.ConnectionFailed;
        }
        instance.conn = conn;
        instance.defaultFormat = allocator.dupeZ(u8, "JSONEachRow") catch return Error.AllocatorOutOfMemory;

        return instance;
    }

    // Currently only JSONEachRow is supported, but this is a placeholder for future formats
    pub fn setDefaultFormat(self: *Connection, format: []u8) void {
        _ = self;
        _ = format;
    }

    pub fn deinit(self: *Connection) void {
        if (self.conn != null) {
            chdb_headers.chdb_close_conn(self.conn);
            self.conn = null;
        }
        self.allocator.free(self.defaultFormat);
        self.allocator.destroy(self);
    }

    pub fn query(self: *Connection, sql: []u8) Error!Result {
        const c_sql = self.allocator.dupeZ(u8, sql) catch return Error.AllocatorOutOfMemory;
        defer self.allocator.free(c_sql);

        const result = chdb_headers.chdb_query(self.conn.*, c_sql.ptr, self.defaultFormat.ptr);
        if (result == null) {
            return Error.QueryFailed;
        }
        return Result{ .res = result };
    }

    pub fn execute(self: *Connection, sql: []u8) Error!void {
        const c_sql = self.allocator.dupeZ(u8, sql) catch return Error.AllocatorOutOfMemory;
        defer self.allocator.free(c_sql);

        const result = chdb_headers.chdb_query(self.conn.*, c_sql.ptr, self.defaultFormat.ptr);
        if (result == null) {
            return Error.QueryFailed;
        }
        var chResult = Result{ .res = result };
        defer chResult.deinit();
        if (!chResult.isSuccess()) {
            std.log.err("QUERY FAILED: {?s}\n", .{chResult.getError()});
            return Error.QueryFailed;
        }
        return;
    }

    pub fn queryStreaming(self: *Connection, sql: []u8) Error!StreamingHandle {
        const c_sql = self.allocator.dupeZ(u8, sql) catch return Error.AllocatorOutOfMemory;
        defer self.allocator.free(c_sql);

        const result = chdb_headers.chdb_stream_query(self.conn.*, c_sql.ptr, self.defaultFormat.ptr);
        if (result == null) {
            return Error.QueryFailed;
        }
        return StreamingHandle{ .handle = result };
    }

    pub fn nextStreamingChunk(self: *Connection, result: *StreamingHandle) Error!Result {
        const res = chdb_headers.chdb_stream_fetch_result(self.conn.*, result.handle);
        if (res == null) {
            return Error.QueryFailed;
        }
        return Result{ .res = res };
    }

    pub fn closeStreaming(self: *Connection, result: *StreamingHandle) void {
        if (result.handle != null) {
            chdb_headers.chdb_stream_cancel_query(self.conn.*, result.handle);
        }
    }
};

pub const StreamingHandle = struct {
    handle: [*c]chdb_headers.struct_chdb_result_,

    pub fn deinit(self: *StreamingHandle) void {
        if (self.handle != null) {
            chdb_headers.chdb_destroy_query_result(self.handle);
            self.handle = null;
        }
    }

    pub fn getError(self: *StreamingHandle) ?[]const u8 {
        if (self.handle == null) {
            return null;
        }
        const err = chdb_headers.chdb_result_error(self.handle);
        if (err == null) {
            return null;
        }
        return std.mem.span(err);
    }

    pub fn isSuccess(self: *StreamingHandle) bool {
        if (self.handle == null) {
            return false;
        }
        return self.getError() == null;
    }
};

pub const Result = struct {
    res: [*c]chdb_headers.struct_chdb_result_,

    pub fn deinit(self: *Result) void {
        if (self.res != null) {
            chdb_headers.chdb_destroy_query_result(self.res);
            self.res = null;
        }
    }

    pub fn data(self: *Result) []const u8 {
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
    pub fn iter(self: *Result, arena: Allocator) Iterator {
        return Iterator.init(self, arena);
    }

    pub fn size(self: *Result) usize {
        if (self.res == null) {
            return 0;
        }
        return chdb_headers.chdb_result_length(self.res);
    }

    pub fn elapsedTime(self: *Result) f64 {
        if (self.res == null) {
            return 0.0;
        }
        return chdb_headers.chdb_result_elapsed(self.res);
    }

    pub fn rowsRead(self: *Result) u64 {
        if (self.res == null) {
            return 0;
        }
        return chdb_headers.chdb_result_rows_read(self.res);
    }

    pub fn bytesRead(self: *Result) u64 {
        if (self.res == null) {
            return 0;
        }
        return chdb_headers.chdb_result_bytes_read(self.res);
    }

    pub fn storageRowsRead(self: *Result) u64 {
        if (self.res == null) {
            return 0;
        }
        return chdb_headers.chdb_result_storage_rows_read(self.res);
    }

    pub fn storageBytesRead(self: *Result) u64 {
        if (self.res == null) {
            return 0;
        }
        return chdb_headers.chdb_result_storage_bytes_read(self.res);
    }

    pub fn getError(self: *Result) ?[]const u8 {
        if (self.res == null) {
            return null;
        }
        const err = chdb_headers.chdb_result_error(self.res);
        if (err == null) {
            return null;
        }
        return std.mem.span(err);
    }

    pub fn isSuccess(self: *Result) bool {
        if (self.res == null) {
            return false;
        }
        return self.getError() == null;
    }
};

/// The Data itself (The Union)
pub fn Data(comptime T: type) type {
    return union(enum) {
        single: T,
        slice: []T,
    };
}

/// The Result Wrapper (The Struct)
pub fn ResultValue(comptime T: type) type {
    return struct {
        data: Data(T),
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
pub const Iterator = struct {
    // We store the raw buffer and use an iterator that doesn't allocate
    iter: std.mem.SplitIterator(u8, .scalar),
    _rowCount: usize,
    currentIndex: usize,
    allocator: Allocator,
    dataSizeInBytes: usize,

    pub fn init(result: *Result, allocator: Allocator) Iterator {
        const data_buf = result.data();
        if (data_buf.len == 0) {
            return Iterator{
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

        return Iterator{
            .iter = std.mem.splitScalar(u8, data_buf, '\n'),
            ._rowCount = count,
            .currentIndex = 0,
            .allocator = allocator,
            .dataSizeInBytes = data_buf.len,
        };
    }

    pub fn maxMemoryUsage(self: *Iterator) usize {
        return self.dataSizeInBytes * 2;
    }

    fn getArenaAllocator(self: *Iterator) Error!*std.heap.ArenaAllocator {
        var arena = self.allocator.create(std.heap.ArenaAllocator) catch return Error.AllocatorOutOfMemory;
        arena.* = std.heap.ArenaAllocator.init(self.allocator);
        errdefer {
            arena.deinit();
            self.allocator.destroy(arena);
        }
        return arena;
    }

    /// Returns the next row as a slice of the original buffer (Zero-copy)
    pub fn nextRow(self: *Iterator) ?[]const u8 {
        while (self.iter.next()) |line| {
            // Chdb sometimes leaves empty lines at the end of NDJSON
            if (line.len == 0) continue;

            self.currentIndex += 1;
            return line;
        }
        return null;
    }

    pub fn nextAs(self: *Iterator, comptime T: type) Error!?ResultValue(T) {
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

    pub fn reset(self: *Iterator) void {
        self.iter.reset();
        self.currentIndex = 0;
    }

    pub fn rowAt(self: *Iterator, index: usize) ?[]const u8 {
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

    pub fn sliceOwned(self: *Iterator, start: usize, end: usize) Error!ResultValue([]const u8) {
        const arena = try self.getArenaAllocator();
        const arena_allocator = arena.allocator();
        var rows: std.ArrayList([]const u8) = .empty;
        self.reset();
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            if (self.currentIndex >= start and self.currentIndex < end) {
                rows.append(arena_allocator, line) catch return Error.AllocatorOutOfMemory;
            }
            self.currentIndex += 1;
            if (self.currentIndex >= end) break;
        }
        const res = rows.toOwnedSlice(arena_allocator) catch return Error.AllocatorOutOfMemory;
        return .{
            .data = .{ .slice = res },
            ._arena = arena,
        };
    }

    pub fn sliceAsOwned(self: *Iterator, comptime T: type, start: usize, end: usize) Error!ResultValue(T) {
        const arena = try self.getArenaAllocator();
        const arena_allocator = arena.allocator();
        var rows: std.ArrayList(T) = .{};
        self.reset();
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            if (self.currentIndex >= start and self.currentIndex < end) {
                const parsed = std.json.parseFromSlice(T, arena_allocator, line, .{ .ignore_unknown_fields = true }) catch {
                    return Error.InvalidRow;
                };

                rows.append(arena_allocator, parsed.value) catch return Error.AllocatorOutOfMemory;
            }
            self.currentIndex += 1;
            if (self.currentIndex >= end) break;
        }
        const res = rows.toOwnedSlice(arena_allocator) catch return Error.AllocatorOutOfMemory;
        return .{
            .data = .{ .slice = res },
            ._arena = arena,
        };
    }

    pub fn takeOwned(self: *Iterator, count: usize) Error![]const []const u8 {
        var rows: std.ArrayList([]const u8) = .{};
        defer rows.deinit(self.allocator);
        var taken: usize = 0;
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            if (taken < count) {
                rows.append(self.allocator, line) catch return Error.AllocatorOutOfMemory;
                taken += 1;
            } else {
                break;
            }
        }
        const res = rows.toOwnedSlice(self.allocator) catch return Error.AllocatorOutOfMemory;
        return res;
    }

    pub fn takeAsOwned(self: *Iterator, comptime T: type, count: usize) Error!ResultValue(T) {
        const arena = try self.getArenaAllocator();
        const arena_allocator = arena.allocator();
        var rows = std.ArrayList(T).initCapacity(arena_allocator, count) catch return Error.AllocatorOutOfMemory;
        var taken: usize = 0;
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            if (taken < count) {
                const parsed = std.json.parseFromSlice(T, arena_allocator, line, .{ .ignore_unknown_fields = true }) catch {
                    return Error.InvalidResult;
                };

                rows.appendAssumeCapacity(parsed.value);
                taken += 1;
            } else {
                break;
            }
        }
        const res = rows.toOwnedSlice(arena_allocator) catch return Error.AllocatorOutOfMemory;
        return .{
            .data = .{ .slice = res },
            ._arena = arena,
        };
    }

    pub fn selectOwned(self: *Iterator, predicate: fn ([]const u8) bool) Error![]const []const u8 {
        var rows: std.ArrayList([]const u8) = .{};
        defer rows.deinit(self.allocator);
        self.reset();
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            if (predicate(line)) {
                rows.append(self.allocator, line) catch break;
            }
        }
        const res = rows.toOwnedSlice(self.allocator) catch return Error.AllocatorOutOfMemory;
        return res;
    }

    pub fn selectAsOwned(self: *Iterator, comptime T: type, predicate: fn (T) bool) Error!ResultValue(T) {
        const arena = try self.getArenaAllocator();
        const arena_allocator = arena.allocator();
        var rows: std.ArrayList(T) = .{};
        self.reset();
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            const parsed = std.json.parseFromSlice(T, arena_allocator, line, .{ .ignore_unknown_fields = true }) catch {
                return Error.InvalidResult;
            };
            if (predicate(parsed.value)) {
                rows.append(arena_allocator, parsed.value) catch break;
            }
        }
        const res = rows.toOwnedSlice(arena_allocator) catch return Error.AllocatorOutOfMemory;
        return .{
            .data = .{ .slice = res },
            ._arena = arena,
        };
    }

    pub fn rowCount(self: *Iterator) usize {
        return self._rowCount;
    }

    /// Peek at the next row without advancing the iterator
    /// Returns null if no more rows
    pub fn peekRow(self: *Iterator) ?[]const u8 {
        var temp_iter = self.iter;
        while (temp_iter.next()) |line| {
            if (line.len == 0) continue;
            return line;
        }
        return null;
    }

    /// Check if there are more rows to iterate
    pub fn hasNextRow(self: *Iterator) bool {
        return self.peekRow() != null;
    }

    /// Get the current iteration index (0-based)
    pub fn getCurrentIndex(self: *Iterator) usize {
        return self.currentIndex;
    }

    /// Collect all remaining rows as owned slices (zero-copy)
    pub fn collectAllOwned(self: *Iterator) Error![]const []const u8 {
        var rows: std.ArrayList([]const u8) = .{};
        defer rows.deinit(self.allocator);
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            rows.append(self.allocator, line) catch return Error.AllocatorOutOfMemory;
        }
        const res = rows.toOwnedSlice(self.allocator) catch return Error.AllocatorOutOfMemory;
        return res;
    }

    /// Collect all remaining rows parsed as type T
    pub fn collectAllAsOwned(self: *Iterator, comptime T: type) Error!ResultValue(T) {
        const arena = try self.getArenaAllocator();
        const arena_allocator = arena.allocator();
        var rows: std.ArrayList(T) = .{};
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            const parsed = std.json.parseFromSlice(T, arena_allocator, line, .{ .ignore_unknown_fields = true }) catch {
                return Error.InvalidResult;
            };
            rows.append(arena_allocator, parsed.value) catch return Error.AllocatorOutOfMemory;
        }
        const res = rows.toOwnedSlice(arena_allocator) catch return Error.AllocatorOutOfMemory;
        return .{
            .data = .{ .slice = res },
            ._arena = arena,
        };
    }

    /// Iterate over each remaining row without allocating
    /// Callback is called for each row
    pub fn forEachRow(self: *Iterator, callback: fn ([]const u8) void) void {
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            self.currentIndex += 1;
            callback(line);
        }
    }

    /// Iterate over each remaining row parsed as type T
    /// Callback is called for each parsed row. Can return errors.
    pub fn forEachRowAs(self: *Iterator, comptime T: type, callback: fn (T) Error!void) Error!void {
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            self.currentIndex += 1;
            const parsed = std.json.parseFromSlice(T, self.allocator, line, .{ .ignore_unknown_fields = true }) catch {
                return Error.InvalidResult;
            };
            defer parsed.deinit();
            try callback(parsed.value);
        }
    }

    /// Count rows matching the predicate
    pub fn countMatching(self: *Iterator, predicate: fn ([]const u8) bool) usize {
        var count: usize = 0;
        self.reset();
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            if (predicate(line)) {
                count += 1;
            }
        }
        return count;
    }

    /// Check if any row matches the predicate (short-circuits on first match)
    pub fn anyMatch(self: *Iterator, predicate: fn ([]const u8) bool) bool {
        self.reset();
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            if (predicate(line)) {
                return true;
            }
        }
        return false;
    }

    /// Check if all rows match the predicate (short-circuits on first mismatch)
    pub fn allMatch(self: *Iterator, predicate: fn ([]const u8) bool) bool {
        self.reset();
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            if (!predicate(line)) {
                return false;
            }
        }
        return true;
    }

    /// Skip N rows, returns the number of rows actually skipped
    pub fn skipRows(self: *Iterator, count: usize) usize {
        var skipped: usize = 0;
        while (skipped < count and self.iter.next() != null) {
            skipped += 1;
        }
        self.currentIndex += skipped;
        return skipped;
    }

    /// Find and return the first row matching the predicate, parsed as type T
    pub fn findFirstAs(self: *Iterator, comptime T: type, predicate: fn (T) bool) Error!?ResultValue(T) {
        while (self.iter.next()) |line| {
            if (line.len == 0) continue;
            self.currentIndex += 1;

            const parsed = std.json.parseFromSlice(T, self.allocator, line, .{ .ignore_unknown_fields = true }) catch {
                continue;
            };

            if (predicate(parsed.value)) {
                const arena = try self.getArenaAllocator();
                const arena_allocator = arena.allocator();
                const cloned = try std.json.parseFromSlice(T, arena_allocator, line, .{ .ignore_unknown_fields = true });
                parsed.deinit();
                return .{
                    .data = .{ .single = cloned.value },
                    ._arena = arena,
                };
            }
            parsed.deinit();
        }
        return null;
    }
};
