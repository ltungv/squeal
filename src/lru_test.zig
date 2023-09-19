const std = @import("std");
const testing = std.testing;
const squeal_lru = @import("lru.zig");

test "lru cache get and set" {
    var cache = squeal_lru.AutoLruCache(u32, u32, 3).init(testing.allocator);
    defer cache.deinit();

    try cache.set(0, 69);
    try cache.set(1, 420);
    try cache.set(2, 666);

    try testing.expectEqual(@as(u32, 69), cache.get(0).?.*);
    try testing.expectEqual(@as(u32, 420), cache.get(1).?.*);
    try testing.expectEqual(@as(u32, 666), cache.get(2).?.*);
}

test "lru cache invalidate least recently used" {
    var cache = squeal_lru.AutoLruCache(u32, u32, 3).init(testing.allocator);
    defer cache.deinit();

    try cache.set(0, 69);
    try cache.set(1, 420);
    try cache.set(2, 666);

    // 0 should be invalidated
    try cache.set(3, 777);
    try testing.expectEqual(@as(?*u32, null), cache.get(0));
    try testing.expectEqual(@as(u32, 777), cache.get(3).?.*);
    // use 1 and 2
    try testing.expectEqual(@as(u32, 420), cache.get(1).?.*);
    try testing.expectEqual(@as(u32, 666), cache.get(2).?.*);
    // 3 should be invalidated
    try cache.set(0, 69);
    try testing.expectEqual(@as(?*u32, null), cache.get(3));
    try testing.expectEqual(@as(u32, 69), cache.get(0).?.*);
}
