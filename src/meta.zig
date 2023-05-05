const std = @import("std");

pub fn sizeOfField(comptime T: type, comptime field: std.meta.FieldEnum(T)) usize {
    const field_info = std.meta.fieldInfo(T, field);
    if (@TypeOf(field_info) == std.builtin.Type.StructField) {
        return @sizeOf(field_info.field_type);
    } else {
        @compileError("can only be used with struct fields.");
    }
}
