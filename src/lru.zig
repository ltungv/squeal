const std = @import("std");

/// A LRU cache with fixed size that uses the predefined hash and eql functions
/// for the key type.
pub fn AutoLruCache(comptime K: type, comptime V: type, comptime SIZE: usize) type {
    if (SIZE == 0) @compileError("cache size must be greater than 0");

    const Entry = struct { key: K, value: V };
    const Dequeue = std.TailQueue(Entry);
    const HashMap = std.AutoHashMapUnmanaged(K, Dequeue.Node);

    return struct {
        allocator: std.mem.Allocator,
        order: Dequeue,
        items: HashMap,

        /// Initialize the cache with the given allocator.
        pub fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .allocator = allocator,
                .order = Dequeue{},
                .items = HashMap{},
            };
        }

        /// Deinitialize the cache.
        pub fn deinit(this: *@This()) void {
            this.items.deinit(this.allocator);
        }

        /// Get a pointer to the value at the given key.
        pub fn getPtr(this: *@This(), key: K) ?*V {
            if (this.items.getPtr(key)) |node| {
                this.order.remove(node);
                this.order.prepend(node);
                return &node.data.value;
            }
            return null;
        }

        /// Get the value at the given key.
        pub fn get(this: *@This(), key: K) ?V {
            if (this.getPtr(key)) |value| return value.*;
            return null;
        }

        /// Set the value at the given key returning any value that was removed.
        /// There are 2 scenarios where a value is removed:
        /// 1. The key already exists and the value is overwritten.
        /// 2. The cache is full and the least recently used value is removed.
        pub fn set(this: *@This(), key: K, value: V) !?V {
            var removed_value: ?V = null;
            var entry = try this.items.getOrPut(this.allocator, key);
            if (entry.found_existing) {
                // We always move the newly added node to the front of the queue,
                // so we need to unlink the node when overwriting its value.
                this.order.remove(entry.value_ptr);
                removed_value = entry.value_ptr.data.value;
            }
            entry.value_ptr.data = .{ .key = key, .value = value };
            this.order.prepend(entry.value_ptr);
            // Handle on cache full.
            if (this.items.size > SIZE) {
                const lru_node = this.order.pop().?;
                removed_value = lru_node.data.value;
                std.debug.assert(this.items.remove(lru_node.data.key));
            }
            return removed_value;
        }
    };
}
