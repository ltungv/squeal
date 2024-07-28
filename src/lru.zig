const std = @import("std");

const TestCache = AutoLruCache(u64, u64);

/// An LRU cache backed by a hash map and a doubly-linked list. Two functions
/// `hash` and `eql` are automatically generated for the key type so it can be
/// used in the hash map. The cache only invalidates entries when asked to and
/// does not automatically remove entries when it's over capacity.
pub fn AutoLruCache(comptime K: type, comptime V: type) type {
    return struct {
        allocator: std.mem.Allocator,
        max_size: usize,
        /// Linked list of keys in order of most-recently used to least-recently used.
        keys_order: TailQueue,
        /// Hash map from keys to values and linked list node references.
        entries: HashMap,

        const TailQueue = std.TailQueue(K);
        const HashMap = std.AutoHashMap(K, HashMapValue);
        const HashMapValue = struct { data: V, node: *TailQueue.Node };

        /// An entry in the cache.
        pub const Entry = struct { key: K, value: V };

        /// Error that can occur when using the cache.
        pub const Error = std.mem.Allocator.Error;

        /// Initialize the cache with an allocator and its max size.
        pub fn init(allocator: std.mem.Allocator, max_size: usize) @This() {
            return .{
                .allocator = allocator,
                .max_size = max_size,
                .keys_order = TailQueue{},
                .entries = HashMap.init(allocator),
            };
        }

        /// Deinitialize the cache.
        pub fn deinit(this: *@This()) void {
            while (this.keys_order.pop()) |node| this.allocator.destroy(node);
            this.entries.deinit();
        }

        /// Get the value at the given key.
        pub fn get(this: *@This(), key: K) ?V {
            if (this.entries.get(key)) |value| {
                this.keys_order.remove(value.node);
                this.keys_order.prepend(value.node);
                return value.data;
            }
            return null;
        }

        /// Set the value at the given key and overwrite any value that is
        /// currently associated with it. The overwritten value is returned.
        pub fn set(this: *@This(), key: K, value: V) Error!?V {
            var entry = try this.entries.getOrPut(key);
            defer {
                entry.value_ptr.data = value;
                this.keys_order.prepend(entry.value_ptr.node);
            }
            if (entry.found_existing) {
                this.keys_order.remove(entry.value_ptr.node);
                return entry.value_ptr.data;
            }
            entry.value_ptr.node = try this.allocator.create(TailQueue.Node);
            entry.value_ptr.node.data = key;
            return null;
        }

        /// Removes the least-recently used entry from the cache and returns it,
        /// if the is over capacity. Otherwise, returns null.
        pub fn invalidate(this: *@This()) Error!?Entry {
            if (this.entries.count() > this.max_size) {
                if (this.keys_order.pop()) |node| {
                    defer this.allocator.destroy(node);
                    if (this.entries.fetchRemove(node.data)) |entry| {
                        return .{ .key = entry.key, .value = entry.value.data };
                    }
                }
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
    try std.testing.expectEqual(@as(?u64, 69), try cache.set(0, 420));
    try std.testing.expectEqual(@as(?u64, 420), try cache.set(1, 666));
    try std.testing.expectEqual(@as(?u64, 666), try cache.set(2, 69));

    // asserts
    try std.testing.expectEqual(@as(?u64, 420), cache.get(0));
    try std.testing.expectEqual(@as(?u64, 666), cache.get(1));
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
    try std.testing.expectEqual(@as(?u64, null), try cache.set(3, 777));
    try std.testing.expectEqual(@as(?u64, null), try cache.set(4, 777));
    try std.testing.expectEqual(@as(?u64, null), try cache.set(5, 777));
    // asserts
    try std.testing.expectEqual(@as(?TestCache.Entry, .{ .key = 0, .value = 69 }), try cache.invalidate());
    try std.testing.expectEqual(@as(?TestCache.Entry, .{ .key = 1, .value = 420 }), try cache.invalidate());
    try std.testing.expectEqual(@as(?TestCache.Entry, .{ .key = 2, .value = 666 }), try cache.invalidate());
    try std.testing.expectEqual(@as(?u64, null), cache.get(2));
    try std.testing.expectEqual(@as(?u64, 777), cache.get(3));
    try std.testing.expectEqual(@as(?u64, 777), cache.get(4));
    try std.testing.expectEqual(@as(?u64, 777), cache.get(5));
}
