const std = @import("std");

pub const chdb = @cImport({
    @cInclude("chdb.h");
});

pub const ChError = error{
    ConnectionFailed,
    NotValid,
    NotFound,
    TypeMismatch,
    IndexOutOfBounds,
};

pub const Row = struct {
    _row: std.json.Parsed(std.json.Value), // Holds the parsed JSON value for the current line

    pub fn deinit(self: *Row) void {
        self._row.deinit();
    }
    pub fn columns(self: *Row) [][]const u8 {
        // TODO: clone the keys
        return self._row.value.object.keys();
    }
    pub fn get(self: *Row, T: type, name: []const u8) ?T {
        const json_value = self._row.value.object.get(name) orelse {
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
    pub fn init(res: [*c]chdb.local_result_v2) !ChSingleRow {
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
