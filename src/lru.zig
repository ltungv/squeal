const std = @import("std");
const assert = std.debug.assert;

/// A LRU cache with fixed size that uses the predefined hash and eql functions
/// for the key type.
pub fn AutoLruCache(comptime K: type, comptime V: type, comptime SIZE: usize) type {
    if (SIZE == 0) @compileError("cache size must be greater than 0");

    return struct {
        allocator: std.mem.Allocator,
        order: Dequeue,
        items: HashMap,

        const Dequeue = std.TailQueue(Entry);
        const HashMap = std.AutoHashMapUnmanaged(K, Dequeue.Node);

        pub const Error = std.mem.Allocator.Error;

        pub const Entry = struct { key: K, value: V };

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
        pub fn set(this: *@This(), key: K, value: V) Error!?Entry {
            var invalidated_entry: ?Entry = null;
            var entry = try this.items.getOrPut(this.allocator, key);
            // If we overwrite an existing value, we need to unlink it from the
            // queue so we can later move it to the front.
            if (entry.found_existing) {
                invalidated_entry = entry.value_ptr.data;
                this.order.remove(entry.value_ptr);
            }
            // Assign data and move entry to the front of the queue.
            entry.value_ptr.data = .{ .key = key, .value = value };
            this.order.prepend(entry.value_ptr);
            // Cache invalidation when full.
            if (this.items.size > SIZE) {
                const lru_node = this.order.pop().?;
                invalidated_entry = lru_node.data;
                assert(this.items.remove(lru_node.data.key));
            }
            return invalidated_entry;
        }
    };
}
