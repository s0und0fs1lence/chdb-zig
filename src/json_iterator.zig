const std = @import("std");
const types = @import("types.zig");
const Row = types.Row;

pub const JsonLineIterator = struct {
    lines: std.mem.TokenIterator(u8, .scalar),
    alloc: std.mem.Allocator,
    total_lines: u64,
    current_line: u64,
    current_row: ?*Row,

    pub fn init(content: []const u8, rows: u64, alloc: std.mem.Allocator) JsonLineIterator {
        return .{
            .lines = std.mem.tokenizeScalar(u8, content, '\n'),
            .alloc = alloc,
            .total_lines = rows,
            .current_line = 0,
            .current_row = null,
        };
    }

    pub fn deinit(self: *JsonLineIterator) void {
        if (self.current_row) |row| {
            row.deinit();
            self.alloc.destroy(row);
            self.current_row = null;
        }
    }

    pub fn next(self: *JsonLineIterator) ?*Row {
        // Clean up previous row if it exists
        if (self.current_row) |row| {
            row.deinit();
            self.alloc.destroy(row);
            self.current_row = null;
        }

        if (self.current_line >= self.total_lines) return null;

        if (self.lines.next()) |line| {
            if (line.len == 0) return null;

            const parsed = std.json.parseFromSlice(std.json.Value, self.alloc, line, .{}) catch return null;
            const row = self.alloc.create(Row) catch return null;
            row.* = Row{ ._row = parsed };
            self.current_line += 1;
            self.current_row = row;
            return row;
        }
        return null;
    }

    pub fn getIndex(self: *JsonLineIterator) usize {
        return @intCast(self.current_line);
    }

    pub fn setIndex(self: *JsonLineIterator, index: usize) !void {
        // Clean up current row if it exists
        if (self.current_row) |row| {
            row.deinit();
            self.alloc.destroy(row);
            self.current_row = null;
        }

        if (index > self.total_lines) return error.IndexOutOfBounds;

        // Reset iterator
        self.lines.reset();
        self.current_line = 0;

        // Advance to desired position
        var i: usize = 0;
        while (i < index) : (i += 1) {
            _ = self.lines.next();
            self.current_line += 1;
        }
    }
};
