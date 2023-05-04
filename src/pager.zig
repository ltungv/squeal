const std = @import("std");
const errors = @import("errors.zig");

const Row = @import("table.zig").Row;

pub const Pager = struct {
    allocator: std.mem.Allocator,
    len: usize,
    file: std.fs.File,
    cache: [MAX_PAGES]?[]u8,

    pub const PAGE_SIZE = 4096;
    pub const MAX_PAGES = 100;
    pub const Error = error{
        OutOfBound,
        NullPage,
        CurrentWorkingDirectoryUnlinked,
    } || std.mem.Allocator.Error || std.fs.File.OpenError || std.os.PReadError || std.os.PWriteError;

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, path: []const u8) Error!Self {
        const file_path = try std.fs.path.resolve(allocator, &[_][]const u8{path});
        defer allocator.free(file_path);

        const file = try std.fs.createFileAbsolute(file_path, .{
            .read = true,
            .truncate = false,
            .exclusive = false,
        });
        const file_stat = try file.stat();

        var cache: [MAX_PAGES]?[]u8 = undefined;
        std.mem.set(?[]u8, &cache, null);

        return Self{
            .allocator = allocator,
            .len = file_stat.size,
            .file = file,
            .cache = cache,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.cache) |*nullable_page| {
            if (nullable_page.*) |page| {
                self.allocator.free(page);
                nullable_page.* = null;
            }
        }
        self.file.close();
    }

    pub fn flush(self: *Self, page_num: usize, num_bytes: usize) Error!void {
        if (self.cache[page_num]) |page| {
            _ = try self.file.pwriteAll(page[0..num_bytes], page_num * PAGE_SIZE);
        } else {
            return Error.NullPage;
        }
    }

    pub fn getPage(self: *Self, page_num: usize) Error![]u8 {
        if (page_num >= MAX_PAGES) {
            return Error.OutOfBound;
        }

        var page: []u8 = undefined;
        if (self.cache[page_num]) |p| {
            page = p;
        } else {
            page = try self.allocator.alloc(u8, PAGE_SIZE);
            var num_pages = self.len / PAGE_SIZE;
            if (self.len % PAGE_SIZE != 0) {
                num_pages += 1;
            }
            if (page_num < num_pages) {
                _ = try self.file.preadAll(page, page_num * PAGE_SIZE);
            }
            self.cache[page_num] = page;
        }

        return page;
    }
};

pub const Node = struct {
    header: NodeHeader,
    body: NodeBody,

    pub const Self = @This();

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.IoError!void {
        try self.header.serialize(stream);
        try self.body.serialize(stream);
    }

    pub fn deserialize(self: *Self, stream: *std.io.FixedBufferStream([]const u8)) errors.IoError!void {
        try self.header.deserialize(stream);
        try self.body.deserialize(stream);
    }
};

pub const NodeHeader = struct {
    is_root: u8,
    parent: u32,

    pub const Self = @This();

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.IoError!void {
        var writer = stream.writer();
        try writer.writeInt(u8, self.is_root, .Little);
        try writer.writeInt(u32, self.parent, .Little);
    }

    pub fn deserialize(self: *Self, stream: *std.io.FixedBufferStream([]const u8)) errors.IoError!void {
        var reader = stream.reader();
        self.is_root = try reader.readInt(u8, .Little);
        self.parent = try reader.readInt(u32, .Little);
    }
};

pub const NodeType = enum(u8) { Leaf };

pub const NodeBody = union(NodeType) {
    Leaf: LeafNode,

    const Self = @This();

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.IoError!void {
        var writer = stream.writer();
        switch (self.*) {
            .Leaf => {
                try writer.writeInt(u8, @enumToInt(NodeType.Leaf), .Little);
                try self.Leaf.serialize(stream);
            },
        }
    }

    pub fn deserialize(self: *Self, stream: *std.io.FixedBufferStream([]const u8)) errors.IoError!void {
        var reader = stream.reader();
        const node_type = try reader.readEnum(NodeType, .Little);
        switch (node_type) {
            .Leaf => {
                var leaf: LeafNode = undefined;
                try leaf.deserialize(stream);
                self.* = .{ .Leaf = leaf };
            },
        }
    }
};

pub const LeafNode = struct {
    num_cells: u32,
    cells: [MAX_CELLS]?Cell,

    pub const MAX_CELLS = (Pager.PAGE_SIZE - @sizeOf(NodeHeader) - @sizeOf(u32)) / @sizeOf(Cell);

    const Self = @This();

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.IoError!void {
        var writer = stream.writer();
        try writer.writeInt(u32, self.num_cells, .Little);
        var cell_index: u32 = 0;
        while (cell_index < self.num_cells) : (cell_index += 1) {
            try self.cells[cell_index].?.serialize(stream);
        }
    }

    pub fn deserialize(self: *Self, stream: *std.io.FixedBufferStream([]const u8)) errors.IoError!void {
        var reader = stream.reader();
        self.num_cells = try reader.readInt(u32, .Little);
        var cell_index: u32 = 0;
        while (cell_index < self.num_cells) : (cell_index += 1) {
            try self.cells[cell_index].?.deserialize(stream);
        }
    }
};

pub const Cell = struct {
    key: u32,
    val: Row,

    const Self = @This();

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.IoError!void {
        var writer = stream.writer();
        try writer.writeInt(u32, self.key, .Little);
        try self.val.serialize(stream);
    }

    pub fn deserialize(self: *Self, stream: *std.io.FixedBufferStream([]const u8)) errors.IoError!void {
        var reader = stream.reader();
        self.key = try reader.readInt(u32, .Little);
        try self.val.deserialize(stream);
    }
};
