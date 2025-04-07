const std = @import("std");
const fmt = std.fmt;
const io = std.io;
const mem = std.mem;
const meta = std.meta;
const testing = std.testing;

pub const QueryBuilderError = error{
    OutOfMemory,
    InvalidFormatString,
    UnknownFormatSpecifier,
    ArgumentCountMismatch,
    ArgumentTypeMismatch,
    InvalidDateFormat,
    InvalidArrayFormat,
};

/// Interpolates arguments into a ClickHouse SQL query format string.
/// Provides strong protection against SQL injection through proper escaping
/// and type checking.
///
/// Placeholders:
///   {s}: String literal (will be escaped and single-quoted)
///   {u}: Unsigned integer
///   {i}: Signed integer
///   {f}: Floating-point number
///   {d}: Date in YYYY-MM-DD format
///   {t}: DateTime in YYYY-MM-DD HH:MM:SS format
///   {b}: Boolean (true/false)
///   {a}: Array (will be properly formatted for ClickHouse)
///   {n}: NULL value
///
/// Args should be a tuple, e.g., .{"hello'world", 123}
pub fn interpolate(
    allocator: mem.Allocator,
    query_fmt: []const u8,
    args: anytype,
) QueryBuilderError![]u8 {
    var out_list = std.ArrayList(u8).init(allocator);
    errdefer out_list.deinit();

    var writer = out_list.writer();
    var arg_idx: usize = 0;
    const fields = std.meta.fields(@TypeOf(args));
    const num_args = fields.len;
    var required_args: usize = 0;

    // First pass: count required arguments
    var i: usize = 0;
    while (i < query_fmt.len) : (i += 1) {
        const byte = query_fmt[i];
        if (byte == '{') {
            if (i + 1 >= query_fmt.len) return QueryBuilderError.InvalidFormatString;
            i += 1;
            if (i + 1 >= query_fmt.len or query_fmt[i + 1] != '}')
                return QueryBuilderError.InvalidFormatString;
            i += 1;
            required_args += 1;
        } else if (byte == '}' and i + 1 < query_fmt.len and query_fmt[i + 1] == '}') {
            i += 1;
        } else if (byte == '}') {
            return QueryBuilderError.InvalidFormatString;
        }
    }

    // Check argument count before proceeding
    if (num_args != required_args) {
        return QueryBuilderError.ArgumentCountMismatch;
    }

    // Second pass: format query
    i = 0;
    arg_idx = 0;
    while (i < query_fmt.len) : (i += 1) {
        const byte = query_fmt[i];

        if (byte == '{') {
            i += 1;
            const specifier = query_fmt[i];
            i += 1;

            inline for (fields, 0..) |field, idx| {
                if (idx == arg_idx) {
                    try formatAndAppendArg(writer, specifier, @field(args, field.name));
                    break;
                }
            }

            arg_idx += 1;
        } else if (byte == '}' and i + 1 < query_fmt.len and query_fmt[i + 1] == '}') {
            try writer.writeByte('}');
            i += 1;
        } else if (byte != '}') {
            try writer.writeByte(byte);
        }
    }

    return out_list.toOwnedSlice();
}

fn formatAndAppendArg(
    writer: anytype,
    specifier: u8,
    arg: anytype,
) QueryBuilderError!void {
    const T = @TypeOf(arg);
    const type_info = @typeInfo(T);

    switch (specifier) {
        's' => {
            var str: []const u8 = undefined;
            switch (type_info) {
                .array => |info| {
                    if (info.child != u8) {
                        return QueryBuilderError.ArgumentTypeMismatch;
                    }
                    str = if (info.sentinel) |_| arg[0..std.mem.len(&arg)] else &arg;
                },
                .pointer => |ptr| {
                    if (ptr.is_const) {
                        str = @ptrCast(arg);
                    } else {
                        if (ptr.child != u8) {
                            return QueryBuilderError.ArgumentTypeMismatch;
                        }
                        str = @ptrCast(arg);
                    }
                },
                else => return QueryBuilderError.ArgumentTypeMismatch,
            }
            try writer.writeByte('\'');
            try escapeClickHouseStringToWriter(writer, str);
            try writer.writeByte('\'');
        },
        'u' => switch (type_info) {
            .int => |int| {
                if (int.signedness != .unsigned) {
                    return QueryBuilderError.ArgumentTypeMismatch;
                }
                try fmt.formatInt(arg, 10, .lower, .{}, writer);
            },
            .comptime_int => {
                if (arg < 0) {
                    return QueryBuilderError.ArgumentTypeMismatch;
                }
                try fmt.formatInt(arg, 10, .lower, .{}, writer);
            },
            else => return QueryBuilderError.ArgumentTypeMismatch,
        },
        'i' => switch (type_info) {
            .int => |int| {
                if (int.signedness != .signed) {
                    return QueryBuilderError.ArgumentTypeMismatch;
                }
                try fmt.formatInt(arg, 10, .lower, .{}, writer);
            },
            .comptime_int => try fmt.formatInt(arg, 10, .lower, .{}, writer),
            else => return QueryBuilderError.ArgumentTypeMismatch,
        },
        'f' => switch (type_info) {
            .float => try fmt.format(writer, "{d}", .{arg}),
            .comptime_float => try fmt.format(writer, "{d}", .{arg}),
            else => return QueryBuilderError.ArgumentTypeMismatch,
        },
        'b' => switch (type_info) {
            .bool => try writer.writeAll(if (arg) "true" else "false"),
            else => return QueryBuilderError.ArgumentTypeMismatch,
        },
        'd', 't' => {
            const str = switch (type_info) {
                .array => |info| blk: {
                    if (info.child != u8) {
                        return QueryBuilderError.ArgumentTypeMismatch;
                    }
                    break :blk if (info.sentinel) |_| arg[0..] else &arg;
                },
                .pointer => |ptr| blk: {
                    if (ptr.is_const) {
                        break :blk @as([]const u8, @ptrCast(arg));
                    }
                    if (ptr.child != u8) {
                        return QueryBuilderError.ArgumentTypeMismatch;
                    }
                    break :blk @as([]const u8, @ptrCast(arg));
                },
                else => return QueryBuilderError.ArgumentTypeMismatch,
            };

            if (specifier == 'd' and !isValidDateString(str)) {
                return QueryBuilderError.InvalidDateFormat;
            }
            if (specifier == 't' and !isValidDateTimeString(str)) {
                return QueryBuilderError.InvalidDateFormat;
            }
            try writer.writeByte('\'');
            try writer.writeAll(str);
            try writer.writeByte('\'');
        },
        'a' => switch (type_info) {
            .array => try formatClickHouseArray(writer, &arg),
            .pointer => |ptr| {
                switch (@typeInfo(ptr.child)) {
                    .array => try formatClickHouseArray(writer, arg[0..]),
                    else => {
                        if (ptr.size != .Slice) {
                            return QueryBuilderError.ArgumentTypeMismatch;
                        }
                        try formatClickHouseArray(writer, arg);
                    },
                }
            },
            else => return QueryBuilderError.ArgumentTypeMismatch,
        },
        'n' => switch (type_info) {
            .null, .optional => try writer.writeAll("NULL"),
            else => return QueryBuilderError.ArgumentTypeMismatch,
        },
        else => return QueryBuilderError.UnknownFormatSpecifier,
    }
}

fn escapeClickHouseStringToWriter(
    writer: anytype,
    input: []const u8,
) !void {
    for (input) |char| {
        switch (char) {
            '\'', '\\' => try writer.writeByte('\\'),
            '\x00' => {
                try writer.writeAll("\\0");
                continue;
            },
            '\n' => {
                try writer.writeAll("\\n");
                continue;
            },
            '\r' => {
                try writer.writeAll("\\r");
                continue;
            },
            '\t' => {
                try writer.writeAll("\\t");
                continue;
            },
            else => {},
        }
        try writer.writeByte(char);
    }
}

fn isValidDateString(date: []const u8) bool {
    if (date.len != 10) return false;
    // YYYY-MM-DD
    return mem.eql(u8, date[4..5], "-") and
        mem.eql(u8, date[7..8], "-") and
        (std.fmt.parseInt(u16, date[0..4], 10) catch return false) > 0 and
        (std.fmt.parseInt(u8, date[5..7], 10) catch return false) > 0 and
        (std.fmt.parseInt(u8, date[8..10], 10) catch return false) > 0;
}

fn isValidDateTimeString(datetime: []const u8) bool {
    if (datetime.len != 19) return false;
    // YYYY-MM-DD HH:MM:SS
    return isValidDateString(datetime[0..10]) and
        mem.eql(u8, datetime[10..11], " ") and
        mem.eql(u8, datetime[13..14], ":") and
        mem.eql(u8, datetime[16..17], ":");
}

fn formatClickHouseArray(writer: anytype, array: anytype) !void {
    const T = @TypeOf(array);
    const type_info = @typeInfo(T);

    try writer.writeByte('[');

    switch (type_info) {
        .array => {
            for (array, 0..) |item, i| {
                if (i > 0) try writer.writeAll(", ");
                try formatArrayElement(writer, item);
            }
        },
        .pointer => |ptr| {
            if (ptr.size == .slice) {
                for (array, 0..) |item, i| {
                    if (i > 0) try writer.writeAll(", ");
                    try formatArrayElement(writer, item);
                }
            } else if (ptr.size == .many) {
                for (array, 0..) |item, i| {
                    if (i > 0) try writer.writeAll(", ");
                    try formatArrayElement(writer, item);
                }
            } else if (ptr.size == .one) {
                for (array, 0..) |item, i| {
                    if (i > 0) try writer.writeAll(", ");
                    try formatArrayElement(writer, item);
                }
            } else {
                return QueryBuilderError.ArgumentTypeMismatch;
            }
        },
        else => return QueryBuilderError.ArgumentTypeMismatch,
    }

    try writer.writeByte(']');
}

fn formatArrayElement(writer: anytype, value: anytype) !void {
    const T = @TypeOf(value);
    const type_info = @typeInfo(T);

    switch (type_info) {
        .int => try fmt.formatInt(value, 10, .lower, .{}, writer),
        .float => try fmt.format(writer, "{d}", .{value}),
        .bool => try writer.writeAll(if (value) "true" else "false"),
        .array => |info| {
            if (info.child != u8) {
                @compileError("Unsupported array element type");
            }
            try writer.writeByte('\'');
            try escapeClickHouseStringToWriter(writer, value[0..]);
            try writer.writeByte('\'');
        },
        .pointer => |ptr| {
            if (ptr.size == .slice and ptr.child == u8) {
                try writer.writeByte('\'');
                try escapeClickHouseStringToWriter(writer, value);
                try writer.writeByte('\'');
            } else {
                @compileError("Unsupported array element type");
            }
        },
        else => @compileError("Unsupported array element type"),
    }
}

test "basic interpolation" {
    const allocator = testing.allocator;
    const name = "Arthur's Seat";
    const id = 42;
    const value = 123.45;

    const sql_fmt = "SELECT * FROM table WHERE name = {s} AND id = {u} AND value > {f};";
    const expected = "SELECT * FROM table WHERE name = 'Arthur\\'s Seat' AND id = 42 AND value > 123.45;";

    const result = try interpolate(allocator, sql_fmt, .{ name, id, value });
    defer allocator.free(result);

    try testing.expectEqualStrings(expected, result);
}

test "advanced sql injection prevention" {
    const allocator = testing.allocator;
    const evil_input = "'; DROP TABLE users; --";

    const sql_fmt = "SELECT * FROM users WHERE name = {s}";
    const expected = "SELECT * FROM users WHERE name = '\\'; DROP TABLE users; --'";

    const result = try interpolate(allocator, sql_fmt, .{evil_input});
    defer allocator.free(result);

    try testing.expectEqualStrings(expected, result);
}

test "array formatting" {
    const allocator = testing.allocator;
    const numbers = [_]i32{ 1, 2, 3 };
    const strings = [_][]const u8{ "hello", "world" };

    {
        const sql_fmt = "SELECT * FROM table WHERE ids IN {a}";
        const expected = "SELECT * FROM table WHERE ids IN [1, 2, 3]";
        const result = try interpolate(allocator, sql_fmt, .{&numbers});
        defer allocator.free(result);
        try testing.expectEqualStrings(expected, result);
    }

    {
        const sql_fmt = "SELECT * FROM table WHERE names IN {a}";
        const expected = "SELECT * FROM table WHERE names IN ['hello', 'world']";
        const result = try interpolate(allocator, sql_fmt, .{&strings});
        defer allocator.free(result);
        try testing.expectEqualStrings(expected, result);
    }
}

test "date and datetime" {
    const allocator = testing.allocator;
    const date = "2025-04-07";
    const datetime = "2025-04-07 13:45:30";

    {
        const sql_fmt = "SELECT * FROM table WHERE date = {d}";
        const expected = "SELECT * FROM table WHERE date = '2025-04-07'";
        const result = try interpolate(allocator, sql_fmt, .{date});
        defer allocator.free(result);
        try testing.expectEqualStrings(expected, result);
    }

    {
        const sql_fmt = "SELECT * FROM table WHERE timestamp = {t}";
        const expected = "SELECT * FROM table WHERE timestamp = '2025-04-07 13:45:30'";
        const result = try interpolate(allocator, sql_fmt, .{datetime});
        defer allocator.free(result);
        try testing.expectEqualStrings(expected, result);
    }
}

test "string escaping" {
    const allocator = testing.allocator;
    const dangerous_string = "' OR 1=1; -- \\";

    const sql_fmt = "INSERT INTO logs (message) VALUES ({s});";
    const expected_sql = "INSERT INTO logs (message) VALUES ('\\' OR 1=1; -- \\\\');";

    const result = try interpolate(allocator, sql_fmt, .{dangerous_string});
    defer allocator.free(result);

    try testing.expectEqualStrings(expected_sql, result);
}

test "integer types" {
    const allocator = testing.allocator;
    const uid: u64 = 9999999999999999999;
    const sid: i32 = -123;

    const sql_fmt = "VALUES ({u}, {i})";
    const expected = "VALUES (9999999999999999999, -123)";

    const result = try interpolate(allocator, sql_fmt, .{ uid, sid });
    defer allocator.free(result);

    try testing.expectEqualStrings(expected, result);
}

test "error: argument count mismatch (too few)" {
    const allocator = testing.allocator;
    const sql_fmt = "SELECT {s}, {i}";
    try testing.expectError(QueryBuilderError.ArgumentCountMismatch, interpolate(allocator, sql_fmt, .{"one"}));
}

test "error: argument count mismatch (too many)" {
    const allocator = testing.allocator;
    const sql_fmt = "SELECT {s}";
    try testing.expectError(QueryBuilderError.ArgumentCountMismatch, interpolate(allocator, sql_fmt, .{ "one", 2 }));
}

test "error: argument type mismatch" {
    const allocator = testing.allocator;
    const sql_fmt = "SELECT {s}"; // Expects string
    try testing.expectError(QueryBuilderError.ArgumentTypeMismatch, interpolate(allocator, sql_fmt, .{123})); // Provide int
}

test "error: unknown format specifier" {
    const allocator = testing.allocator;
    const sql_fmt = "SELECT {x}";
    try testing.expectError(QueryBuilderError.UnknownFormatSpecifier, interpolate(allocator, sql_fmt, .{123}));
}

test "error: invalid format string" {
    const allocator = testing.allocator;
    try testing.expectError(QueryBuilderError.InvalidFormatString, interpolate(allocator, "SELECT {s", .{"a"}));
    try testing.expectError(QueryBuilderError.InvalidFormatString, interpolate(allocator, "SELECT {", .{}));
    try testing.expectError(QueryBuilderError.InvalidFormatString, interpolate(allocator, "SELECT }", .{}));
    try testing.expectError(QueryBuilderError.InvalidFormatString, interpolate(allocator, "SELECT {s{}", .{ "a", "b" }));
}
