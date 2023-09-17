const std = @import("std");

// A node within a B+ tree, which can be one of two types:
// + Leaf node containing data entries and their keys.
// + Internal node containing key indices and pointers to child node.
pub fn Node(comptime T: type, comptime S: usize) type {
    return extern struct {
        header: NodeHeader,
        body: NodeBody(T, S),
    };
}

// Type of a node in a B+ tree.
pub const NodeType = enum(u8) {
    Leaf,
    Internal,
};

// Header of a node in a B+ tree containing its metadata.
pub const NodeHeader = extern struct {
    parent: u32,
    is_root: bool,
    type: NodeType,
};

// Body of a node in a B+ tree, which can be one of two types leaf or internal.
// The node type is determined explicitly by the header insteading of using
// Zig's tagged union.
pub fn NodeBody(comptime T: type, comptime S: usize) type {
    return extern union {
        leaf: NodeLeaf(T, S),
        internal: NodeInternal(S),
    };
}

// Content of a leaf node in a B+ tree.
pub fn NodeLeaf(comptime T: type, comptime S: usize) type {
    return extern struct {
        next_leaf: u32,
        num_cells: u32,
        cells: [MAX_CELLS]NodeCell(T),

        // Max number of data cells a leaf node can hold.
        pub const MAX_CELLS = (S - @sizeOf(NodeHeader) - @sizeOf(u32) * 2) / @sizeOf(NodeCell(T));
        // Number of cells in the right leaf node after splitting.
        pub const R_SPLIT_CELLS = (MAX_CELLS + 1) / 2;
        // Number of cells in the left leaf node after splitting.
        pub const L_SPLIT_CELLS = (MAX_CELLS + 1) - R_SPLIT_CELLS;

        // Find the index of the cell with the given key using binary search.
        // If there's no cell with the given key, an index of where the cell
        // should be is returned.
        pub fn find(this: *const @This(), key: u32) u32 {
            return searchCells(T, this.cells[0..this.num_cells], key);
        }
    };
}

// Content of an internal node in a B+ tree.
pub fn NodeInternal(comptime S: usize) type {
    return extern struct {
        right_child: u32,
        num_keys: u32,
        cells: [MAX_KEYS]NodeCell(u32),

        // Max number of data cells an internal node can hold.
        pub const MAX_KEYS = (S - @sizeOf(NodeHeader) - @sizeOf(u32) * 2) / @sizeOf(NodeCell(u32));
        // Number of cells in the right internal node after splitting.
        pub const R_SPLIT_KEYS = (MAX_KEYS + 1) / 2;
        // Number of cells in the left internal node after splitting.
        pub const L_SPLIT_KEYS = (MAX_KEYS + 1) - R_SPLIT_KEYS;

        // Return the child node index at the given index.
        pub fn getChild(this: *const @This(), index: u32) u32 {
            if (index == this.num_keys) return this.right_child;
            return this.cells[index].val;
        }

        // Find the index of the cell with the given key using binary search.
        // If there's no cell with the given key, an index of where the cell
        // should be is returned.
        pub fn find(this: *const @This(), key: u32) u32 {
            return searchCells(u32, this.cells[0..this.num_keys], key);
        }

        // Find the index at old_key and update its value to new_key.
        pub fn updateKey(this: *@This(), old_key: u32, new_key: u32) void {
            const index = this.find(old_key);
            if (index < this.num_keys) {
                this.cells[index].key = new_key;
            }
        }
    };
}

// A data cell within a node in a B+ tree.
pub fn NodeCell(comptime T: type) type {
    return extern struct {
        key: u32,
        val: T,
    };
}

/// A pager is responsible for reading and writing pages (blocks of data) to a file.
/// Changes made on a page are not persisted until the page is flushed.
pub fn Pager(comptime T: type, comptime PAGE_SIZE: u32, comptime PAGE_COUNT: u32) type {
    return struct {
        allocator: std.mem.Allocator,
        len: u32,
        file: std.fs.File,
        page_count: u32,
        page_cache: [PAGE_COUNT]?*Node(T, PAGE_SIZE),

        /// Error that occurs when using a pager.
        pub const Error = error{
            Corrupted,
            EndOfStream,
            FileSystem,
            NotSupported,
            NullPage,
            OutOfBound,
        } ||
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
            const file_size: u32 = @intCast(file_stat.size);
            if (file_size % PAGE_SIZE != 0) return Error.Corrupted;
            // Initialize all cached pages to null.
            var pages: [PAGE_COUNT]?*Node(T, PAGE_SIZE) = undefined;
            @memset(&pages, null);
            return .{
                .allocator = allocator,
                .len = file_size,
                .file = file,
                .page_count = file_size / PAGE_SIZE,
                .page_cache = pages,
            };
        }

        /// Deinitialize the pager. This flushes all pages to disk and frees any allocated memory.
        pub fn deinit(this: *@This()) void {
            for (&this.page_cache) |*page| {
                if (page.*) |the_page| {
                    this.allocator.destroy(the_page);
                    page.* = null;
                }
            }
            this.file.close();
        }

        /// Flush a page to disk.
        pub fn flush(this: *@This(), page_num: u32) Error!void {
            const page = this.page_cache[page_num] orelse return Error.NullPage;
            var buf: [PAGE_SIZE]u8 = undefined;
            var stream = std.io.fixedBufferStream(&buf);
            var writer = stream.writer();
            try writer.writeStruct(page.*);
            _ = try this.file.pwriteAll(&buf, page_num * PAGE_SIZE);
        }

        /// Get a pointer to a cached page. If the page is not in cache, it will be read from disk.
        pub fn get(this: *@This(), page_num: u32) Error!*Node(T, PAGE_SIZE) {
            if (page_num >= PAGE_COUNT) return Error.OutOfBound;
            if (this.page_cache[page_num]) |node| return node;
            // Cache miss.
            var page = try this.allocator.create(Node(T, PAGE_SIZE));
            const pages_on_disk = this.len / PAGE_SIZE;
            if (page_num < pages_on_disk) {
                // Load page from disk if it exists.
                var page_buf: [PAGE_SIZE]u8 = undefined;
                _ = try this.file.preadAll(&page_buf, page_num * PAGE_SIZE);
                // Deserialize page into its in-memory representation.
                var stream = std.io.fixedBufferStream(&page_buf);
                var reader = stream.reader();
                page.* = try reader.readStruct(Node(T, PAGE_SIZE));
            }
            // Update in memory page count to match the number of pages on disk.
            if (page_num >= this.page_count) this.page_count = page_num + 1;
            // Cache page.
            this.page_cache[page_num] = page;
            return page;
        }

        // Get a free page.
        pub fn getFree(this: *const @This()) u32 {
            // TODO: Smarter strategy so deallocated page can be reused.
            return this.page_count;
        }
    };
}

fn searchCells(comptime T: type, cells: []const NodeCell(T), key: u32) u32 {
    var left: u32 = 0;
    var right: u32 = @intCast(cells.len);
    while (left < right) {
        const index = (left + right) / 2;
        const cell = cells[index];
        if (key == cell.key) return index;
        if (key < cell.key) right = index else left = index + 1;
    }
    return left;
}
