const std = @import("std");
const testing = std.testing;

const Pager = @import("pager.zig").Pager;
const Node = @import("pager.zig").Node;
const NodeLeaf = @import("pager.zig").NodeLeaf;

const TestPager = Pager(u32, 4096, 64);

test "node size check" {
    try testing.expect(@sizeOf(Node(u8, 4096)) <= 4096);
    try testing.expect(@sizeOf(Node(u16, 4096)) <= 4096);
    try testing.expect(@sizeOf(Node(u32, 4096)) <= 4096);
    try testing.expect(@sizeOf(Node(u64, 4096)) <= 4096);

    try testing.expect(@sizeOf(Node(i8, 4096)) <= 4096);
    try testing.expect(@sizeOf(Node(i16, 4096)) <= 4096);
    try testing.expect(@sizeOf(Node(i32, 4096)) <= 4096);
    try testing.expect(@sizeOf(Node(i64, 4096)) <= 4096);

    try testing.expect(@sizeOf(Node(f32, 4096)) <= 4096);
    try testing.expect(@sizeOf(Node(f64, 4096)) <= 4096);
}

test "page init" {
    var pager = try TestPager.init(testing.allocator, "test.squeal");
    defer pager.deinit();
}

test "page flush null page" {
    var pager = try TestPager.init(testing.allocator, "test.squeal");
    defer pager.deinit();

    try testing.expectError(error.NullPage, pager.flush(0));
}

test "page flush persist page" {
    {
        var pager = try TestPager.init(testing.allocator, "./test.squeal");
        defer pager.deinit();

        var node = try pager.get(0);
        node.header.parent = 420;
        try pager.flush(0);
    }

    var pager = try TestPager.init(testing.allocator, "./test.squeal");
    defer pager.deinit();

    var node = try pager.get(0);
    try testing.expectEqual(@intCast(u32, 420), node.header.parent);
}
