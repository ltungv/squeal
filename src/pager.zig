const std = @import("std");
const debug = std.debug;
const squeal_lru = @import("lru.zig");

/// A node within a B+ tree, which can be one of two types:
/// + Leaf node containing data entries and their keys.
/// + Internal node containing key indices and pointers to child node.
pub fn Node(comptime T: type, comptime S: usize) type {
    return extern struct {
        header: NodeHeader,
        body: NodeBody(T, S),

        /// Initialize a node.
        pub fn init(parent: u64, is_root: bool, node_type: NodeType) @This() {
            return .{
                .header = NodeHeader.init(parent, is_root, node_type),
                .body = NodeBody(T, S).init(node_type),
            };
        }
    };
}

/// Type of a node in a B+ tree.
pub const NodeType = enum(u8) {
    Leaf,
    Internal,
};

/// Header of a node in a B+ tree containing its metadata.
pub const NodeHeader = extern struct {
    parent: u64,
    is_root: bool,
    type: NodeType,

    /// Initialize a node header.
    pub fn init(parent: u64, is_root: bool, node_type: NodeType) @This() {
        return .{ .parent = parent, .is_root = is_root, .type = node_type };
    }
};

/// Body of a node in a B+ tree, which can be one of two types leaf or internal.
/// The node type is determined explicitly by the header insteading of using
/// Zig's tagged union.
pub fn NodeBody(comptime T: type, comptime S: usize) type {
    return extern union {
        leaf: NodeLeaf(T, S),
        internal: NodeInternal(S),

        /// Initialize a node body.
        pub fn init(node_type: NodeType) @This() {
            switch (node_type) {
                .Leaf => return .{ .leaf = NodeLeaf(T, S).init() },
                .Internal => return .{ .internal = NodeInternal(S).init() },
            }
        }
    };
}

/// Content of a leaf node in a B+ tree.
pub fn NodeLeaf(comptime T: type, comptime S: usize) type {
    return extern struct {
        next_leaf: u64,
        num_cells: u64,
        cells: [MAX_CELLS]NodeCell(T),

        /// Max number of data cells a leaf node can hold.
        pub const MAX_CELLS = (S - @sizeOf(NodeHeader) - @sizeOf(u64) * 2) / @sizeOf(NodeCell(T));
        /// Number of cells in the right leaf node after splitting.
        pub const R_SPLIT_CELLS = (MAX_CELLS + 1) / 2;
        /// Number of cells in the left leaf node after splitting.
        pub const L_SPLIT_CELLS = (MAX_CELLS + 1) - R_SPLIT_CELLS;

        /// Initialize a leaf node.
        pub fn init() @This() {
            return .{ .next_leaf = 0, .num_cells = 0, .cells = undefined };
        }

        /// Find the index of the cell with the given key using binary search.
        /// If there's no cell with the given key, an index of where the cell
        /// should be is returned.
        pub fn find(this: *const @This(), key: u64) u64 {
            return searchCells(T, this.cells[0..this.num_cells], key);
        }
    };
}

/// Content of an internal node in a B+ tree.
pub fn NodeInternal(comptime S: usize) type {
    return extern struct {
        right_child: u64,
        num_keys: u64,
        cells: [MAX_KEYS]NodeCell(u64),

        /// Max number of data cells an internal node can hold.
        pub const MAX_KEYS = (S - @sizeOf(NodeHeader) - @sizeOf(u64) * 2) / @sizeOf(NodeCell(u64));
        /// Number of cells in the right internal node after splitting.
        pub const R_SPLIT_KEYS = (MAX_KEYS + 1) / 2;
        /// Number of cells in the left internal node after splitting.
        pub const L_SPLIT_KEYS = (MAX_KEYS + 1) - R_SPLIT_KEYS;

        /// Initialize an internal node.
        pub fn init() @This() {
            return .{ .right_child = 0, .num_keys = 0, .cells = undefined };
        }

        /// Return the child node index at the given index.
        pub fn getChild(this: *const @This(), index: u64) u64 {
            if (index == this.num_keys) return this.right_child;
            return this.cells[index].val;
        }

        /// Find the index of the cell with the given key using binary search.
        /// If there's no cell with the given key, an index of where the cell
        /// should be is returned.
        pub fn find(this: *const @This(), key: u64) u64 {
            return searchCells(u64, this.cells[0..this.num_keys], key);
        }

        /// Find the index at old_key and update its value to new_key.
        pub fn updateKey(this: *@This(), old_key: u64, new_key: u64) void {
            const index = this.find(old_key);
            if (index < this.num_keys) this.cells[index].key = new_key;
        }
    };
}

/// A data cell within a node in a B+ tree.
pub fn NodeCell(comptime T: type) type {
    return extern struct {
        key: u64,
        val: T,
    };
}

/// A pager is responsible for reading and writing pages (blocks of data) to a file.
/// Changes made on a page are not persisted until the page is flushed.
pub fn Pager(comptime T: type, comptime PAGE_SIZE: u64, comptime PAGE_COUNT: u64) type {
    return struct {
        allocator: std.mem.Allocator,
        len: u64,
        file: std.fs.File,
        page_count: u64,
        page_cache: Cache,

        const Cache = squeal_lru.AutoLruCache(u64, *Node(T, PAGE_SIZE));

        /// Error that occurs when using a pager.
        pub const Error = error{ Corrupted, EndOfStream, FileSystem, NotSupported, NullPage, OutOfBound } ||
            std.fs.File.OpenError ||
            std.mem.Allocator.Error ||
            std.os.GetCwdError ||
            std.os.PReadError ||
            std.os.PWriteError;

        /// Create a new pager backed by the given allocator and file.
        pub fn init(allocator: std.mem.Allocator, path: []const u8) Error!@This() {
            const file = try std.fs.cwd().createFile(path, .{
                .read = true,
                .truncate = false,
                .exclusive = false,
            });
            // File must contain whole page(s).
            const file_stat = try file.stat();
            if (file_stat.size % PAGE_SIZE != 0) return Error.Corrupted;
            return .{
                .allocator = allocator,
                .len = file_stat.size,
                .file = file,
                .page_count = file_stat.size / PAGE_SIZE,
                .page_cache = Cache.init(allocator, 128),
            };
        }

        /// Deinitialize the pager.
        pub fn deinit(this: *@This()) void {
            var cache_values_it = this.page_cache.entries.valueIterator();
            while (cache_values_it.next()) |value| this.allocator.destroy(value.data);
            this.page_cache.deinit();
            this.file.close();
        }

        /// Flush a page to disk.
        pub fn flush(this: *@This(), page_num: u64) Error!void {
            const page = this.page_cache.get(page_num) orelse return Error.NullPage;
            try this.writePage(page_num, page);
        }

        /// Flush all known pages to disk.
        pub fn flushAll(this: *@This()) Error!void {
            var page_num: u64 = 0;
            while (page_num < this.page_count) : (page_num += 1) {
                this.flush(page_num) catch |err| if (err != error.NullPage) return err;
            }
        }

        /// Get a pointer to a cached page. If the page is not in cache, it will be read from disk.
        pub fn get(this: *@This(), page_num: u64) Error!*Node(T, PAGE_SIZE) {
            if (page_num >= PAGE_COUNT) return Error.OutOfBound;
            if (this.page_cache.get(page_num)) |page| return page;
            // Cache miss, load page from disk if it exists.
            var page = try this.allocator.create(Node(T, PAGE_SIZE));
            if (page_num < this.len / PAGE_SIZE) {
                var page_buf: [PAGE_SIZE]u8 = undefined;
                const read_bytes = try this.file.preadAll(&page_buf, page_num * PAGE_SIZE);
                debug.assert(read_bytes == PAGE_SIZE);
                var stream = std.io.fixedBufferStream(&page_buf);
                var reader = stream.reader();
                page.* = try reader.readStruct(Node(T, PAGE_SIZE));
            }
            // Write page to memory.
            if (try this.page_cache.set(page_num, page)) |replaced_page| {
                try this.writePage(page_num, replaced_page);
                this.allocator.destroy(replaced_page);
            }
            if (page_num >= this.page_count) this.page_count = page_num + 1;
            return page;
        }

        /// Get a free page.
        pub fn getFree(this: *const @This()) u64 {
            return this.page_count;
        }

        /// Clean up the pager by finding all cached pages that will be evicted
        /// and flush them to disk.
        pub fn clean(this: *@This()) Error!void {
            while (try this.page_cache.invalidate()) |invalidated| {
                try this.writePage(invalidated.key, invalidated.value);
                this.allocator.destroy(invalidated.value);
            }
        }

        /// Write a whole page to disk at the correct offset based on the page number.
        fn writePage(this: *@This(), page_num: u64, page: *const Node(T, PAGE_SIZE)) Error!void {
            var buf: [PAGE_SIZE]u8 = undefined;
            var stream = std.io.fixedBufferStream(&buf);
            var writer = stream.writer();
            try writer.writeStruct(page.*);
            try this.file.pwriteAll(&buf, page_num * PAGE_SIZE);
            this.len = @max(this.len, page_num * PAGE_SIZE + PAGE_SIZE);
        }
    };
}

fn searchCells(comptime T: type, cells: []const NodeCell(T), key: u64) u64 {
    var left: u64 = 0;
    var right: u64 = cells.len;
    while (left < right) {
        const index = (left + right) / 2;
        const cell = cells[index];
        if (key == cell.key) return index;
        if (key < cell.key) right = index else left = index + 1;
    }
    return left;
}
