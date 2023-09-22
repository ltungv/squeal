const std = @import("std");
const debug = std.debug;

/// An LRU cache backed by a hash map and a doubly-linked list. Two functions
/// `hash` and `eql` are automatically generated for the key type so it can be
/// used in the hash map. The cache only invalidates entries when asked to.
pub fn AutoLruCache(comptime K: type, comptime V: type) type {
    return struct {
        allocator: std.mem.Allocator,
        max_size: usize,
        /// A hash map keeping cache entries along with a reference to their
        /// nodes in the doubly-linked list.
        entries: HashMap,
        /// A doubly-linked list that keeps track of the order in which keys are
        /// used recently.
        keys_order: Dequeue,

        const Dequeue = std.TailQueue(K);
        const OrderedValue = struct { data: V, node: *Dequeue.Node };
        const HashMap = std.AutoHashMap(K, OrderedValue);

        /// An entry in the cache.
        pub const Entry = struct { key: K, value: V };

        /// Error that can occur when using the cache.
        pub const Error = std.mem.Allocator.Error;

        /// Initialize the cache with an allocator and its max size.
        pub fn init(allocator: std.mem.Allocator, max_size: usize) @This() {
            return .{
                .allocator = allocator,
                .max_size = max_size,
                .entries = HashMap.init(allocator),
                .keys_order = Dequeue{},
            };
        }

        /// Deinitialize the cache.
        pub fn deinit(this: *@This()) void {
            var values_it = this.entries.valueIterator();
            while (values_it.next()) |value| this.allocator.destroy(value.node);
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
            var replaced_value: ?V = null;
            var entry = try this.entries.getOrPut(key);
            if (entry.found_existing) {
                replaced_value = entry.value_ptr.data;
                this.keys_order.remove(entry.value_ptr.node);
            } else {
                entry.value_ptr.node = try this.allocator.create(Dequeue.Node);
                entry.value_ptr.node.data = key;
            }
            entry.value_ptr.data = value;
            this.keys_order.prepend(entry.value_ptr.node);
            return replaced_value;
        }

        /// Removes the least-recently used entry from the cache and returns it.
        /// If the cache is not over capacity, null is returned.
        pub fn invalidate(this: *@This()) Error!?Entry {
            if (this.entries.count() <= this.max_size) return null;
            if (this.keys_order.pop()) |node| {
                if (this.entries.fetchRemove(node.data)) |entry| {
                    this.allocator.destroy(entry.value.node);
                    return .{ .key = entry.key, .value = entry.value.data };
                }
            }
            return null;
        }
    };
}
