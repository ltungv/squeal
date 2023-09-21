const std = @import("std");
const testing = std.testing;
const squeal_lru = @import("lru.zig");

const TestCache = squeal_lru.AutoLruCache(u64, u64);

test "lru cache get and set" {
    var cache = try TestCache.init(testing.allocator, 3);
    defer cache.deinit();
    // set new
    try cache.set(0, 69);
    try cache.set(1, 420);
    try cache.set(2, 666);
    // asserts
    try testing.expectEqual(@as(?u64, 69), cache.get(0));
    try testing.expectEqual(@as(?u64, 420), cache.get(1));
    try testing.expectEqual(@as(?u64, 666), cache.get(2));
    // overwriting values
    try cache.set(0, 420);
    try cache.set(1, 666);
    try cache.set(2, 69);
    // asserts
    try testing.expectEqual(@as(?u64, 420), cache.get(0));
    try testing.expectEqual(@as(?u64, 666), cache.get(1));
    try testing.expectEqual(@as(?u64, 69), cache.get(2));
}

test "lru cache invalidation order" {
    var cache = try TestCache.init(testing.allocator, 3);
    defer cache.deinit();
    // set new
    try cache.set(0, 69);
    try cache.set(1, 420);
    try cache.set(2, 666);
    // invalidating values
    try cache.set(3, 777);
    try cache.set(4, 777);
    try cache.set(5, 777);
    // asserts
    try testing.expectEqual(@as(?TestCache.Entry, .{ .key = 0, .value = 69 }), try cache.invalidate());
    try testing.expectEqual(@as(?TestCache.Entry, .{ .key = 1, .value = 420 }), try cache.invalidate());
    try testing.expectEqual(@as(?TestCache.Entry, .{ .key = 2, .value = 666 }), try cache.invalidate());
    try testing.expectEqual(@as(?u64, null), cache.get(2));
    try testing.expectEqual(@as(?u64, 777), cache.get(3));
    try testing.expectEqual(@as(?u64, 777), cache.get(4));
    try testing.expectEqual(@as(?u64, 777), cache.get(5));
}
