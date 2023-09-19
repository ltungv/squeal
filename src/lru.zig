const std = @import("std");
const debug = std.debug;
const mem = std.mem;
const testing = std.testing;

/// A simple LRU cache implementation that uses a hash map to track items, and
/// a tail queue to track their order. This implementation automatically
/// provides a `Context` with default implementation of `eql` and `hash` for
/// the type `K`.
pub fn AutoLruCache(comptime K: type, comptime V: type, comptime SIZE: usize) type {
    if (SIZE == 0) @compileError("cache size must be greater than 0");

    const KV = struct { key: K, val: V };
    const TailQueue = std.TailQueue(KV);
    const HashMap = std.AutoHashMapUnmanaged(u32, *TailQueue.Node);

    return struct {
        allocator: mem.Allocator,
        items: HashMap,
        order: TailQueue,

        const Error = mem.Allocator.Error;

        /// Initialize a new LRU cache backed by given allocator.
        pub fn init(allocator: mem.Allocator) @This() {
            return .{
                .allocator = allocator,
                .items = HashMap{},
                .order = TailQueue{},
            };
        }

        /// Deinitialize the LRU cache.
        pub fn deinit(this: *@This()) void {
            this.items.deinit(this.allocator);
            var it = this.order.first;
            while (it) |node| {
                it = node.next;
                this.allocator.destroy(node);
            }
        }

        /// Set a key-value pair in the cache while invalidating the
        /// least-recently used item if the cache is full. This promotes the
        /// inserted pair to the front of the cache.
        pub fn set(this: *@This(), key: K, val: V) Error!void {
            var node: *TailQueue.Node = undefined;
            if (this.items.size >= SIZE) {
                node = this.order.pop().?;
                debug.assert(this.items.remove(node.data.key));
            } else {
                node = try this.allocator.create(TailQueue.Node);
            }
            node.data = .{ .key = key, .val = val };
            try this.items.put(this.allocator, key, node);
            this.order.prepend(node);
        }

        /// Get the value currently associated with the given key, or null if
        /// none is found. This promotes the item to the front of the cache.
        pub fn get(this: *@This(), key: K) ?*V {
            if (this.items.get(key)) |node| {
                this.order.remove(node);
                this.order.prepend(node);
                return &node.data.val;
            }
            return null;
        }
    };
}
