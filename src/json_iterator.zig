const std = @import("std");

const Row = @import("types.zig").Row; // Import the Row struct from types.zig

pub const JsonLineIterator = struct {
    buffer: []const u8, // The entire JSON buffer to be processed
    lines: std.mem.SplitIterator(u8, std.mem.DelimiterType.scalar), // An iterator that yields each line from the buffer
    allocator: std.mem.Allocator, // An allocator to be used for parsing JSON within each line
    curLine: usize,
    rowCount: usize, // The number of rows in the buffer
    pub fn init(buffer: []const u8, rowCount: usize, allocator: std.mem.Allocator) JsonLineIterator {
        return .{
            .buffer = buffer,
            .rowCount = rowCount,
            .lines = std.mem.splitScalar(u8, buffer, '\n'),
            .allocator = allocator,
            .curLine = 0,
        };
    }

    pub fn next(self: *JsonLineIterator) ?*Row {
        if (self.lines.next()) |line| {
            self.curLine += 1;
            const parsed_value = std.json.parseFromSlice(std.json.Value, self.allocator, line, .{}) catch {
                return null;
            };
            // Attempt to parse the current line as a JSON object using std.json.parseFromSlice
            const r = self.allocator.create(Row) catch {
                return null;
            };
            r._row = parsed_value;
            return r;
        }
        return null; // Signal the end of the buffer
    }

    pub fn setIndex(self: *JsonLineIterator, index: usize) !void {
        // reset the iterator to count the lines

        if (index > self.rowCount) {
            return error.IndexOutOfBounds;
        }
        if (index < 0) {
            return error.IndexOutOfBounds;
        }

        if (index <= self.curLine) {
            // if the index is less than the current line, reset the iterator
            // otherwise, just continue from the current position
            self.lines.reset();
        }

        // count the lines
        var cnt: usize = 0;
        var prevIndex = self.lines.index;
        var isValid = false;
        while (self.lines.next()) |_| {
            if (cnt == index) {
                self.lines.index = prevIndex;

                isValid = true;
                break;
            }
            cnt += 1;
            prevIndex = self.lines.index;
        }
        if (!isValid) {
            return error.IndexOutOfBounds;
        }
        self.curLine = index;
    }

    pub fn getIndex(self: *JsonLineIterator) usize {
        return self.curLine;
    }
    pub fn count(self: *JsonLineIterator) usize {
        return self.rowCount;
    }
};
