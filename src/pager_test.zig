const std = @import("std");
const testing = std.testing;
const squeal_pager = @import("pager.zig");

const TestPager = squeal_pager.Pager(u32, 4096, 64);

test "node size check" {
    try testing.expect(@sizeOf(squeal_pager.Node(u8, 4096)) <= 4096);
    try testing.expect(@sizeOf(squeal_pager.Node(u16, 4096)) <= 4096);
    try testing.expect(@sizeOf(squeal_pager.Node(u32, 4096)) <= 4096);
    try testing.expect(@sizeOf(squeal_pager.Node(u64, 4096)) <= 4096);

    try testing.expect(@sizeOf(squeal_pager.Node(i8, 4096)) <= 4096);
    try testing.expect(@sizeOf(squeal_pager.Node(i16, 4096)) <= 4096);
    try testing.expect(@sizeOf(squeal_pager.Node(i32, 4096)) <= 4096);
    try testing.expect(@sizeOf(squeal_pager.Node(i64, 4096)) <= 4096);

    try testing.expect(@sizeOf(squeal_pager.Node(f32, 4096)) <= 4096);
    try testing.expect(@sizeOf(squeal_pager.Node(f64, 4096)) <= 4096);
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
    try testing.expectEqual(@as(u32, @intCast(420)), node.header.parent);
}
