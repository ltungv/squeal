const std = @import("std");
const mem = std.mem;

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

        pub const MAX_CELLS = (S - HEADER_SIZE) / @sizeOf(NodeCell(T));
        pub const R_SPLIT_CELLS = (MAX_CELLS + 1) / 2;
        pub const L_SPLIT_CELLS = (MAX_CELLS + 1) - R_SPLIT_CELLS;

        const HEADER_SIZE = @sizeOf(NodeHeader) + @sizeOf(u32) * 2;
        const Self = @This();

        pub fn find(self: *const Self, key: u32) u32 {
            var left: u32 = 0;
            var right = self.num_cells;
            while (left < right) {
                const index = (left + right) / 2;
                const cell = self.cells[index];
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

        pub const MAX_KEYS = (S - HEADER_SIZE) / @sizeOf(NodeCell(u32));
        pub const R_SPLIT_KEYS = (MAX_KEYS + 1) / 2;
        pub const L_SPLIT_KEYS = (MAX_KEYS + 1) - R_SPLIT_KEYS;

        const HEADER_SIZE = @sizeOf(NodeHeader) + @sizeOf(u32) * 2;
        const Self = @This();

        pub fn getChild(self: *const Self, index: u32) u32 {
            if (index == self.num_keys) return self.right_child;
            return self.cells[index].val;
        }

        pub fn find(self: *const Self, key: u32) u32 {
            var left: u32 = 0;
            var right = self.num_keys;
            while (left < right) {
                const index = (left + right) / 2;
                const cell = self.cells[index];
                if (key == cell.key) return index;
                if (key < cell.key) right = index else left = index + 1;
            }
            return left;
        }

        pub fn updateKey(self: *Self, old_key: u32, new_key: u32) void {
            const old_child_index = self.find(old_key);
            if (old_child_index < self.num_keys) {
                self.cells[old_child_index].key = new_key;
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
pub fn Pager(comptime T: type, comptime PAGE_SIZE: u32, comptime PAGE_MAX_COUNT: u32) type {
    return struct {
        allocator: std.mem.Allocator,
        len: u32,
        file: std.fs.File,
        page_count: u32,
        page_cache: [PAGE_MAX_COUNT]?*Node(T, PAGE_SIZE),

        /// Error that occurs when using a pager.
        pub const Error = error{
            OutOfBound,
            NullPage,
            Corrupted,
            EndOfStream,
        } ||
            std.mem.Allocator.Error ||
            std.fs.File.OpenError ||
            std.os.PReadError ||
            std.os.PWriteError ||
            std.os.GetCwdError;

        const Self = @This();

        /// Create a new pager backed by the given allocator and file.
        pub fn init(allocator: mem.Allocator, path: []const u8) Error!Self {
            // Zig's file system API requires an absolute path, so we need to resolve first.
            // The user-given path can be either a absolute or relative.
            const file_path = try std.fs.path.resolve(allocator, &[_][]const u8{path});
            defer allocator.free(file_path);

            const file = try std.fs.createFileAbsolute(file_path, .{
                .read = true,
                .truncate = false,
                .exclusive = false,
            });

            // File must contain whole page(s).
            const file_stat = try file.stat();
            const file_size = @intCast(u32, file_stat.size);
            if (file_size % PAGE_SIZE != 0) return Error.Corrupted;

            // Initialize all cached pages to null.
            var pages: [PAGE_MAX_COUNT]?*Node(T, PAGE_SIZE) = undefined;
            std.mem.set(?*Node(T, PAGE_SIZE), &pages, null);

            return Self{
                .allocator = allocator,
                .len = file_size,
                .file = file,
                .page_count = file_size / PAGE_SIZE,
                .page_cache = pages,
            };
        }
        /// Deinitialize the pager. This flushes all pages to disk and frees any allocated memory.
        pub fn deinit(self: *Self) void {
            for (self.page_cache) |*page| {
                if (page.*) |the_page| {
                    self.allocator.destroy(the_page);
                    page.* = null;
                }
            }
            self.file.close();
        }

        /// Flush a page to disk.
        pub fn flush(self: *Self, page_num: u32) Error!void {
            const page = self.page_cache[page_num] orelse return Error.NullPage;
            var buf: [PAGE_SIZE]u8 = undefined;
            var stream = std.io.fixedBufferStream(&buf);
            var writer = stream.writer();
            try writer.writeStruct(page.*);
            _ = try self.file.pwriteAll(&buf, page_num * PAGE_SIZE);
        }

        /// Get a pointer to a cached page. If the page is not in cache, it will be read from disk.
        pub fn get(self: *Self, page_num: u32) Error!*Node(T, PAGE_SIZE) {
            if (page_num >= PAGE_MAX_COUNT) return Error.OutOfBound;
            if (self.page_cache[page_num]) |node| return node;

            var page = try self.allocator.create(Node(T, PAGE_SIZE));
            const pages_on_disk = self.len / PAGE_SIZE;
            if (page_num < pages_on_disk) {
                // Load page from disk if it exists.
                var page_buf: [PAGE_SIZE]u8 = undefined;
                _ = try self.file.preadAll(&page_buf, page_num * PAGE_SIZE);
                // Deserialize page into its in-memory representation.
                var stream = std.io.fixedBufferStream(@as([]const u8, &page_buf));
                var reader = stream.reader();
                page.* = try reader.readStruct(Node(T, PAGE_SIZE));
            }
            if (page_num >= self.page_count) self.page_count = page_num + 1;
            self.page_cache[page_num] = page;

            return page;
        }

        pub fn getFreePage(self: *const Self) u32 {
            return self.page_count;
        }
    };
}
