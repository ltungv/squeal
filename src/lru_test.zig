const std = @import("std");
const testing = std.testing;
const squeal_lru = @import("lru.zig");

const TestCache = squeal_lru.AutoLruCache(u64, u64, 3);

test "lru cache get and set" {
    var cache = TestCache.init(testing.allocator);
    defer cache.deinit();
    // set new
    try testing.expectEqual(@as(?TestCache.Entry, null), try cache.set(0, 69));
    try testing.expectEqual(@as(?TestCache.Entry, null), try cache.set(1, 420));
    try testing.expectEqual(@as(?TestCache.Entry, null), try cache.set(2, 666));
    // asserts
    try testing.expectEqual(@as(?u64, 69), cache.get(0));
    try testing.expectEqual(@as(?u64, 420), cache.get(1));
    try testing.expectEqual(@as(?u64, 666), cache.get(2));
    // set through pointers
    cache.getPtr(0).?.* = 420;
    cache.getPtr(1).?.* = 666;
    cache.getPtr(2).?.* = 69;
    // assert
    try testing.expectEqual(@as(?u64, 420), cache.get(0));
    try testing.expectEqual(@as(?u64, 666), cache.get(1));
    try testing.expectEqual(@as(?u64, 69), cache.get(2));
    // overwriting values
    try testing.expectEqual(@as(?TestCache.Entry, .{ .key = 0, .value = 420 }), try cache.set(0, 69));
    try testing.expectEqual(@as(?TestCache.Entry, .{ .key = 1, .value = 666 }), try cache.set(1, 420));
    try testing.expectEqual(@as(?TestCache.Entry, .{ .key = 2, .value = 69 }), try cache.set(2, 666));
}

test "lru cache invalidation order" {
    var cache = squeal_lru.AutoLruCache(u64, u64, 3).init(testing.allocator);
    defer cache.deinit();
    // set new
    try testing.expectEqual(@as(?TestCache.Entry, null), try cache.set(0, 69));
    try testing.expectEqual(@as(?TestCache.Entry, null), try cache.set(1, 420));
    try testing.expectEqual(@as(?TestCache.Entry, null), try cache.set(2, 666));
    // set new invaliding key 0
    try testing.expectEqual(@as(?TestCache.Entry, .{ .key = 0, .value = 69 }), try cache.set(3, 777));
    try testing.expectEqual(@as(?u64, 777), cache.get(3));
    try testing.expectEqual(@as(?u64, null), cache.get(0));
}
