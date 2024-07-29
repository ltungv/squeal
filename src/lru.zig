const std = @import("std");

const TestCache = AutoLruCache(u64, u64);

/// An least-recently used cache backed by a hash map and a doubly-linked list.
pub fn AutoLruCache(comptime K: type, comptime V: type) type {
    return struct {
        /// The cache's allocator.
        allocator: std.mem.Allocator,
        /// The cache's capacity.
        capacity: usize,
        /// The cache's entries.
        entries: HashMap,
        /// The cache's order.
        recency: TailQueue,

        const HashMap = std.AutoHashMap(K, struct { data: V, node: *TailQueue.Node });
        const TailQueue = std.TailQueue(K);

        /// The cache enntry type.
        pub const Entry = struct { key: K, value: V };

        /// The cache error type.
        pub const Error = std.mem.Allocator.Error;

        /// Initialize the cache with an allocator and its max size.
        pub fn init(allocator: std.mem.Allocator, capacity: usize) @This() {
            std.debug.assert(capacity > 0);
            return .{
                .allocator = allocator,
                .capacity = capacity,
                .entries = HashMap.init(allocator),
                .recency = TailQueue{},
            };
        }

        /// Deinitialize the cache.
        pub fn deinit(this: *@This()) void {
            while (this.recency.pop()) |node| this.allocator.destroy(node);
            this.entries.deinit();
        }

        /// Get the value at the given key.
        pub fn get(this: *@This(), key: K) ?V {
            if (this.entries.get(key)) |value| {
                this.recency.remove(value.node);
                this.recency.prepend(value.node);
                return value.data;
            }
            return null;
        }

        /// Set the value at the given key and overwrite any value that is
        /// currently associated with it.
        ///
        /// + If there's an existing value associated with the key, it's
        /// overwritten and returned.
        /// + If the cache is full, the least-recently accesssed value is
        /// removed and returned.
        pub fn set(this: *@This(), key: K, value: V) Error!?V {
            var entry = try this.entries.getOrPut(key);
            if (entry.found_existing) {
                const old_value = entry.value_ptr.data;
                entry.value_ptr.data = value;
                this.recency.remove(entry.value_ptr.node);
                this.recency.prepend(entry.value_ptr.node);
                return old_value;
            }
            entry.value_ptr.data = value;
            entry.value_ptr.node = try this.allocator.create(TailQueue.Node);
            entry.value_ptr.node.data = key;
            this.recency.prepend(entry.value_ptr.node);
            if (this.entries.count() > this.capacity) {
                const least_recently_used_entry_node = this.recency.pop().?;
                const least_recently_used_entry = this.entries.fetchRemove(least_recently_used_entry_node.data).?;
                this.allocator.destroy(least_recently_used_entry_node);
                return least_recently_used_entry.value.data;
            }
            return null;
        }
    };
}

test "lru cache get and set" {
    var cache = TestCache.init(std.testing.allocator, 3);
    defer cache.deinit();
    // set new
    try std.testing.expectEqual(@as(?u64, null), try cache.set(0, 69));
    try std.testing.expectEqual(@as(?u64, null), try cache.set(1, 420));
    try std.testing.expectEqual(@as(?u64, null), try cache.set(2, 666));
    // asserts
    try std.testing.expectEqual(@as(?u64, 69), cache.get(0));
    try std.testing.expectEqual(@as(?u64, 420), cache.get(1));
    try std.testing.expectEqual(@as(?u64, 666), cache.get(2));
    // overwriting values
    try std.testing.expectEqual(@as(?u64, 69), try cache.set(0, 666));
    try std.testing.expectEqual(@as(?u64, 420), try cache.set(1, 420));
    try std.testing.expectEqual(@as(?u64, 666), try cache.set(2, 69));
    // asserts
    try std.testing.expectEqual(@as(?u64, 666), cache.get(0));
    try std.testing.expectEqual(@as(?u64, 420), cache.get(1));
    try std.testing.expectEqual(@as(?u64, 69), cache.get(2));
}

test "lru cache invalidation order" {
    var cache = TestCache.init(std.testing.allocator, 3);
    defer cache.deinit();
    // set new
    try std.testing.expectEqual(@as(?u64, null), try cache.set(0, 69));
    try std.testing.expectEqual(@as(?u64, null), try cache.set(1, 420));
    try std.testing.expectEqual(@as(?u64, null), try cache.set(2, 666));
    // invalidating values
    try std.testing.expectEqual(@as(?u64, 69), try cache.set(3, 777));
    try std.testing.expectEqual(@as(?u64, 420), try cache.set(4, 777));
    try std.testing.expectEqual(@as(?u64, 666), try cache.set(5, 777));
    // asserts
    try std.testing.expectEqual(@as(?u64, null), cache.get(0));
    try std.testing.expectEqual(@as(?u64, null), cache.get(1));
    try std.testing.expectEqual(@as(?u64, null), cache.get(2));
    try std.testing.expectEqual(@as(?u64, 777), cache.get(3));
    try std.testing.expectEqual(@as(?u64, 777), cache.get(4));
    try std.testing.expectEqual(@as(?u64, 777), cache.get(5));
}
