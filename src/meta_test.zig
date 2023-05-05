const std = @import("std");
const meta = @import("meta.zig");
const pager = @import("pager.zig");
const table = @import("table.zig");

const NodeHeader = pager.NodeHeader;
const LeafNode = pager.LeafNode;
const Row = table.Row;

test "sizeOfField" {
    try std.testing.expectEqual(1, comptime meta.sizeOfField(NodeHeader, .is_root));
    try std.testing.expectEqual(4, comptime meta.sizeOfField(NodeHeader, .parent));

    try std.testing.expectEqual((1 << 5), comptime meta.sizeOfField(table.Row, .key_buf));
    try std.testing.expectEqual((1 << 8) - 1, comptime meta.sizeOfField(table.Row, .val_buf));
}
