const std = @import("std");
const debug = std.debug;

/// A fixed size LRU cache that uses the predefined hash and eql functions for
/// the key type.
pub fn AutoLruCache(comptime K: type, comptime V: type, comptime SIZE: usize) type {
    if (SIZE == 0) @compileError("cache size must be greater than 0");
    return struct {
        allocator: std.mem.Allocator,
        entries_order: Dequeue,
        order_node_map: HashMap,

        const Dequeue = std.TailQueue(Entry);
        const HashMap = std.AutoHashMapUnmanaged(K, *Dequeue.Node);

        /// A cache entry consisting of a key and a value.
        pub const Entry = struct { key: K, value: V };

        /// Error that can occur when using the cache.
        pub const Error = std.mem.Allocator.Error;

        /// Initialize the cache.
        pub fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .allocator = allocator,
                .entries_order = Dequeue{},
                .order_node_map = HashMap{},
            };
        }

        /// Deinitialize the cache.
        pub fn deinit(this: *@This()) void {
            var node_it = this.order_node_map.valueIterator();
            while (node_it.next()) |node| this.allocator.destroy(node.*);
            this.order_node_map.deinit(this.allocator);
        }

        /// Get the value at the given key.
        pub fn get(this: *@This(), key: K) ?V {
            if (this.order_node_map.get(key)) |node| {
                this.entries_order.remove(node);
                this.entries_order.prepend(node);
                return node.data.value;
            }
            return null;
        }

        /// Set the value at the given key returning any value that was removed.
        /// There are 2 scenarios where a value is removed:
        /// 1. The key already exists and the value is overwritten.
        /// 2. The cache is full and the least recently used value is removed.
        pub fn set(this: *@This(), key: K, value: V) Error!?Entry {
            var invalidated: ?Entry = null;
            var node_map_entry = try this.order_node_map.getOrPut(this.allocator, key);
            if (node_map_entry.found_existing) {
                invalidated = node_map_entry.value_ptr.*.data;
                this.entries_order.remove(node_map_entry.value_ptr.*);
            } else {
                node_map_entry.value_ptr.* = try this.allocator.create(Dequeue.Node);
                node_map_entry.value_ptr.*.prev = null;
                node_map_entry.value_ptr.*.next = null;
            }
            node_map_entry.value_ptr.*.data = .{ .key = key, .value = value };
            this.entries_order.prepend(node_map_entry.value_ptr.*);
            if (this.order_node_map.size > SIZE) {
                const node_lru = this.entries_order.pop().?;
                invalidated = node_lru.data;
                debug.assert(this.order_node_map.remove(node_lru.data.key));
                this.allocator.destroy(node_lru);
            }
            return invalidated;
        }
    };
}
