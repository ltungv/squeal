const std = @import("std");
const testing = std.testing;
const squeal_pager = @import("pager.zig");
const squeal_table = @import("table.zig");

const PAGE_SIZE = 4096;
const TestPage = squeal_table.Node(u64, PAGE_SIZE);
const TestPager = squeal_pager.Pager(TestPage, PAGE_SIZE, 64);

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
    try testing.expectEqual(@as(u64, @intCast(420)), node.header.parent);
}
