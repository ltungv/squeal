const std = @import("std");
const meta = @import("meta.zig");
const pager = @import("pager.zig");
const table = @import("table.zig");

const NodeHeader = pager.NodeHeader;
const LeafNode = pager.LeafNode;
const Row = table.Row;

test "sizeOfField" {
    const TestStruct = struct {
        a: bool,
        b: u8,
        c: u16,
        d: u32,
        e: u64,
        f: [8]u8,
        g: [4]u16,
        h: [2]u32,
        i: [1]u64,
    };

    try std.testing.expectEqual(1, comptime meta.sizeOfField(TestStruct, .a));
    try std.testing.expectEqual(1, comptime meta.sizeOfField(TestStruct, .b));
    try std.testing.expectEqual(2, comptime meta.sizeOfField(TestStruct, .c));
    try std.testing.expectEqual(4, comptime meta.sizeOfField(TestStruct, .d));
    try std.testing.expectEqual(8, comptime meta.sizeOfField(TestStruct, .e));
    try std.testing.expectEqual(8, comptime meta.sizeOfField(TestStruct, .f));
    try std.testing.expectEqual(8, comptime meta.sizeOfField(TestStruct, .g));
    try std.testing.expectEqual(8, comptime meta.sizeOfField(TestStruct, .h));
    try std.testing.expectEqual(8, comptime meta.sizeOfField(TestStruct, .i));
}
