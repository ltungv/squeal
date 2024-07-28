const std = @import("std");
const debug = std.debug;
const squeal_lru = @import("lru.zig");

/// A pager is responsible for reading and writing pages (blocks of data) to a file.
/// Changes made on a page are not persisted until the page is flushed.
pub fn Pager(comptime T: type, comptime PAGE_SIZE: u64, comptime PAGE_COUNT: u64) type {
    if (@sizeOf(T) > PAGE_SIZE) @compileError("size of type is large than the page size");
    return struct {
        allocator: std.mem.Allocator,
        len: u64,
        file: std.fs.File,
        page_count: u64,
        page_cache: Cache,

        const Cache = squeal_lru.AutoLruCache(u64, *T);

        /// Error that occurs when using a pager.
        pub const Error = error{ Corrupted, EndOfStream, FileSystem, NotSupported, NullPage, OutOfBound } ||
            std.fs.File.OpenError ||
            std.fs.File.PReadError ||
            std.fs.File.PWriteError ||
            std.mem.Allocator.Error ||
            std.process.GetCwdError;

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
        pub fn get(this: *@This(), page_num: u64) Error!*T {
            if (page_num >= PAGE_COUNT) return Error.OutOfBound;
            if (this.page_cache.get(page_num)) |page| return page;
            // Cache miss, load page from disk if it exists.
            const page = try this.allocator.create(T);
            if (page_num < this.len / PAGE_SIZE) {
                var page_buf: [PAGE_SIZE]u8 = undefined;
                const read_bytes = try this.file.preadAll(&page_buf, page_num * PAGE_SIZE);
                debug.assert(read_bytes == PAGE_SIZE);
                var stream = std.io.fixedBufferStream(&page_buf);
                var reader = stream.reader();
                page.* = try reader.readStruct(T);
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
        fn writePage(this: *@This(), page_num: u64, page: *const T) Error!void {
            var buf: [PAGE_SIZE]u8 = undefined;
            var stream = std.io.fixedBufferStream(&buf);
            var writer = stream.writer();
            try writer.writeStruct(page.*);
            try this.file.pwriteAll(&buf, page_num * PAGE_SIZE);
            this.len = @max(this.len, page_num * PAGE_SIZE + PAGE_SIZE);
        }
    };
}
