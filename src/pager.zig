const std = @import("std");

pub fn Node(comptime T: type, comptime S: usize) type {
    return extern struct {
        header: NodeHeader,
        body: NodeBody(T, S),
    };
}

pub const NodeHeader = extern struct {
    parent: u32,
    is_root: bool,
    is_leaf: bool,
};

pub fn NodeBody(comptime T: type, comptime S: usize) type {
    return extern union {
        leaf: NodeLeaf(T, S),
        internal: NodeInternal(S),
    };
}

pub fn NodeLeaf(comptime T: type, comptime S: usize) type {
    return extern struct {
        next_leaf: u32,
        num_cells: u32,
        cells: [MAX_CELLS]NodeCell(T),

        pub const MAX_CELLS = (S - @sizeOf(NodeHeader) - @sizeOf(u32) * 2) / @sizeOf(NodeCell(T));
        pub const R_SPLIT_CELLS = (MAX_CELLS + 1) / 2;
        pub const L_SPLIT_CELLS = (MAX_CELLS + 1) - R_SPLIT_CELLS;

        pub fn find(this: *const @This(), key: u32) u32 {
            var left: u32 = 0;
            var right = this.num_cells;
            while (left < right) {
                const index = (left + right) / 2;
                const cell = this.cells[index];
                if (key == cell.key) return index;
                if (key < cell.key) right = index else left = index + 1;
            }
            return left;
        }
    };
}

pub fn NodeInternal(comptime S: usize) type {
    return extern struct {
        right_child: u32,
        num_keys: u32,
        cells: [MAX_KEYS]NodeCell(u32),

        pub const MAX_KEYS = (S - @sizeOf(NodeHeader) - @sizeOf(u32) * 2) / @sizeOf(NodeCell(u32));
        pub const R_SPLIT_KEYS = (MAX_KEYS + 1) / 2;
        pub const L_SPLIT_KEYS = (MAX_KEYS + 1) - R_SPLIT_KEYS;

        pub fn getChild(this: *const @This(), index: u32) u32 {
            if (index == this.num_keys) return this.right_child;
            return this.cells[index].val;
        }

        pub fn find(this: *const @This(), key: u32) u32 {
            var left: u32 = 0;
            var right = this.num_keys;
            while (left < right) {
                const index = (left + right) / 2;
                const cell = this.cells[index];
                if (key == cell.key) return index;
                if (key < cell.key) right = index else left = index + 1;
            }
            return left;
        }

        pub fn updateKey(this: *@This(), old_key: u32, new_key: u32) void {
            const old_child_index = this.find(old_key);
            if (old_child_index < this.num_keys) {
                this.cells[old_child_index].key = new_key;
            }
        }
    };
}

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
            OutOfBound,
            NullPage,
            Corrupted,
            EndOfStream,
            NotSupported,
            FileSystem,
        } ||
            std.mem.Allocator.Error ||
            std.fs.File.OpenError ||
            std.os.PReadError ||
            std.os.PWriteError ||
            std.os.GetCwdError;

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
            if (page_num >= this.page_count) this.page_count = page_num + 1;
            this.page_cache[page_num] = page;

            return page;
        }

        pub fn getFree(this: *const @This()) u32 {
            return this.page_count;
        }
    };
}
