const std = @import("std");
const JsonLineIterator = @import("json_iterator.zig").JsonLineIterator;
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

pub const ChQueryResult = struct {
    result: *chdb.local_result_v2,
    alloc: std.mem.Allocator,
    json_iterator: ?JsonLineIterator,
    total_rows: usize,
    current_row: usize,

    pub fn init(res: *chdb.local_result_v2, alloc: std.mem.Allocator) !*ChQueryResult {
        const rows = res.rows_read;
        const instance = try alloc.create(ChQueryResult);
        instance.* = .{
            .result = res,
            .alloc = alloc,
            .json_iterator = if (res.buf != null) JsonLineIterator.init(std.mem.span(res.buf), rows, alloc) else null,
            .total_rows = rows,
            .current_row = 0,
        };
        return instance;
    }

    pub fn next(self: *ChQueryResult) ?*Row {
        if (self.json_iterator) |*iterator| {
            return iterator.next();
        }
        return null;
    }

    pub fn getIndex(self: *ChQueryResult) usize {
        if (self.json_iterator) |*iterator| {
            return iterator.getIndex();
        }
        return 0;
    }

    pub fn setIndex(self: *ChQueryResult, index: usize) !void {
        if (self.json_iterator) |*iterator| {
            try iterator.setIndex(index);
        }
    }

    pub fn free(self: *ChQueryResult) void {
        if (self.json_iterator) |*iterator| {
            iterator.deinit();
        }
        chdb.free_result_v2(self.result);
        self.alloc.destroy(self);
    }
};

pub const ToOwnedError = error{
    IndexMismatch,
    EmptyResultSet,
    OutOfMemory,
    /// A value's type retrieved from the Row is incompatible with the target RowT field's type.
    TypeMismatch,
    /// A non-optional field in RowT required a value, but the corresponding Row value was null.
    NullValueForNonOptionalField,
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

    fn getColumnValue(self: *Row, name: []const u8) ?std.json.Value {
        // Get the value of the column with the given name
        return self._row.value.object.get(name);
    }
    /// Converts the current Row instance to a new struct of type RowT
    /// using the provided allocator. The new struct is populated with values
    /// from the current Row instance based on matching field names.
    /// The function returns a pointer to the new struct instance or an error
    pub fn toOwned(self: *Row, alloc: std.mem.Allocator, comptime RowT: type) ToOwnedError!*RowT {
        comptime {
            const RowTInfo = @typeInfo(RowT);
            switch (RowTInfo) {
                // Case where RowT is a struct
                .@"struct" => |info| {
                    // Optional: Add check to ensure it's NOT a tuple if needed
                    if (info.is_tuple) {
                        @compileError("toOwned: RowT cannot be a tuple, must be a struct");
                    }
                    // If we are here, RowT is a non-tuple struct, proceed.
                },
                // Case for any other type tag
                else => |_| {
                    // Use @tagName on RowTInfo directly
                    @compileError("toOwned: RowT must be a struct type, found " ++ @tagName(RowTInfo));
                },
            }
        }
        // 2. Allocate the new struct instance
        const instance_ptr = try alloc.create(RowT);
        // If initialization fails later, ensure we destroy the allocated memory
        // Note: This defer assumes default initialization is sufficient cleanup if an
        // error occurs mid-population. If fields need specific deinit, more
        // complex error handling might be needed.
        errdefer alloc.destroy(instance_ptr);

        // 3. Iterate through fields of the TARGET struct (RowT)
        inline for (std.meta.fields(RowT)) |field| {
            // 4. Try to get the corresponding value from the SOURCE row data using the field name
            if (self.getColumnValue(field.name)) |source_value| {
                // Value exists for this field name in the source row

                const FieldType = field.type;
                // const FieldTypeInfo = @typeInfo(FieldType);
                // const SourceValueType = @TypeOf(source_value);
                // const SourceValueTypeInfo = @typeInfo(SourceValueType);

                // Get pointer to the field within the newly allocated instance
                const field_ptr = &@field(instance_ptr.*, field.name);

                switch (FieldType) {
                    u8 => {
                        field_ptr.* = source_value.string;
                    },
                    i8 => {
                        field_ptr.* = @intCast(source_value.integer);
                    },
                    i16 => {
                        field_ptr.* = @intCast(source_value.integer);
                    },
                    i32 => {
                        field_ptr.* = @intCast(source_value.integer);
                    },
                    i64 => {
                        field_ptr.* = @intCast(source_value.integer);
                    },
                    u16 => {
                        field_ptr.* = @bitCast(source_value.integer);
                    },
                    u32 => {
                        field_ptr.* = @bitCast(source_value.integer);
                    },
                    u64 => {
                        field_ptr.* = @bitCast(source_value.integer);
                    },
                    f32 => {
                        field_ptr.* = @floatCast(source_value.float);
                    },
                    f64 => {
                        field_ptr.* = source_value.float;
                    },
                    []u8 => {
                        field_ptr.* = @constCast(source_value.string);
                    },
                    []const u8 => {
                        field_ptr.* = source_value.string;
                    },
                    bool => {
                        field_ptr.* = source_value.bool;
                    },
                    else => {
                        // Handle other types or error
                        std.debug.print("UnsupportedType: Field '{s}' ({any})\n", .{ field.name, FieldType });
                        return ToOwnedError.TypeMismatch;
                    },
                }
                //     // 5. Check type compatibility and assign (using logic similar to scanStruct)
                //     switch (FieldTypeInfo) {
                //         .optional => |optional_info| {
                //             const TargetInnerType = optional_info.child;
                //             switch (SourceValueTypeInfo) {
                //                 .Optional => |source_optional_info| {
                //                     // Source is ?S, Target is ?T
                //                     if (source_optional_info.child != TargetInnerType) {
                //                         std.debug.print("TypeMismatch: Field '{s}' (?{any}) != Source (?{any})\n", .{ field.name, TargetInnerType, source_optional_info.child });
                //                         return ToOwnedError.TypeMismatch;
                //                     }
                //                     field_ptr.* = source_value; // Assign ?S to ?T (where S == T)
                //                 },
                //                 else => {
                //                     // Source is S, Target is ?T
                //                     if (SourceValueType != TargetInnerType) {
                //                         std.debug.print("TypeMismatch: Field '{s}' (?{any}) != Source ({any})\n", .{ field.name, TargetInnerType, SourceValueType });
                //                         return ToOwnedError.TypeMismatch;
                //                     }
                //                     field_ptr.* = source_value; // Assign S to ?T (where S == T)
                //                 },
                //             }
                //         },
                //         else => { // Target field is non-optional (T)
                //             switch (SourceValueTypeInfo) {
                //                 .optional => {
                //                     // Source is ?S, Target is T
                //                     if (source_value == null) {
                //                         // Trying to assign null to a non-optional field
                //                         std.debug.print("NullValueForNonOptionalField: Field '{s}' ({any}) received null\n", .{ field.name, FieldType });
                //                         return ToOwnedError.NullValueForNonOptionalField;
                //                     }
                //                     // If source is non-null optional ?S, check if S matches T
                //                     const SourceInnerType = SourceValueTypeInfo.Optional.child;
                //                     if (SourceInnerType != FieldType) {
                //                         std.debug.print("TypeMismatch: Field '{s}' ({any}) != Source (?{any} non-null)\n", .{ field.name, FieldType, SourceInnerType });
                //                         return ToOwnedError.TypeMismatch;
                //                     }
                //                     // Assign the unwrapped value S to T
                //                     field_ptr.* = source_value.?;
                //                 },
                //                 else => {
                //                     // Source is S, Target is T
                //                     if (SourceValueType != FieldType) {
                //                         std.debug.print("TypeMismatch: Field '{s}' ({any}) != Source ({any})\n", .{ field.name, FieldType, SourceValueType });
                //                         return ToOwnedError.TypeMismatch;
                //                     }
                //                     field_ptr.* = source_value; // Assign S to T
                //                 },
                //             }
                //         },
                //     }
                // } else {
                //     // Column corresponding to field.name not found in the source Row.
                //     // The field instance_ptr.*.field.name remains uninitialized
                //     // (or default initialized if the struct RowT has defaults).
                //     // std.debug.print("Field '{s}' not found in source row, skipping.\n", .{field.name});
            }
        }

        // 6. Return the populated instance
        return instance_ptr;
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
            bool => @as(T, json_value.bool),
            else => null,
        };
    }
};

pub const ChSingleRow = struct {
    result: *chdb.local_result_v2,
    len: usize,
    rows_read: u64,

    pub fn init(res: *chdb.local_result_v2) ChSingleRow {
        return ChSingleRow{
            .result = res,
            .len = if (res.buf != null) res.len else 0,
            .rows_read = res.rows_read,
        };
    }
};
