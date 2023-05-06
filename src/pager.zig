const std = @import("std");
const errors = @import("errors.zig");
const meta = @import("meta.zig");

const Row = @import("table.zig").Row;
const Table = @import("table.zig").Table;

pub const Pager = struct {
    allocator: std.mem.Allocator,
    len: usize,
    file: std.fs.File,
    pages: usize,
    cache: [MAX_PAGES]?*Node,

    pub const PAGE_SIZE = 4096;
    pub const MAX_PAGES = 100;

    pub const Error = error{
        OutOfBound,
        NullPage,
        Corrupted,
    } ||
        std.mem.Allocator.Error ||
        std.fs.File.OpenError ||
        std.os.PReadError ||
        std.os.PWriteError ||
        std.os.GetCwdError ||
        errors.SerializeError ||
        errors.DeserializeError;

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

        // File must contain whole page(s)
        if (file_stat.size % PAGE_SIZE != 0) {
            return Error.Corrupted;
        }

        var cache: [MAX_PAGES]?*Node = undefined;
        std.mem.set(?*Node, &cache, null);

        return Self{
            .allocator = allocator,
            .len = file_stat.size,
            .file = file,
            .pages = file_stat.size / PAGE_SIZE,
            .cache = cache,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.cache) |*nullable_page| {
            if (nullable_page.*) |page| {
                self.allocator.destroy(page);
                nullable_page.* = null;
            }
        }
        self.file.close();
    }

    pub fn flush(self: *Self, page_num: usize) Error!void {
        if (self.cache[page_num]) |page| {
            var buf: [PAGE_SIZE]u8 = undefined;
            var stream = std.io.fixedBufferStream(&buf);
            try page.serialize(&stream);
            _ = try self.file.pwriteAll(&buf, page_num * PAGE_SIZE);
        } else {
            return Error.NullPage;
        }
    }

    pub fn getPage(self: *Self, page_num: usize) Error!*Node {
        if (page_num >= MAX_PAGES) {
            return Error.OutOfBound;
        }

        var page: *Node = undefined;
        if (self.cache[page_num]) |p| {
            page = p;
        } else {
            page = try self.allocator.create(Node);
            const num_pages = self.len / PAGE_SIZE;
            if (page_num < num_pages) {
                var page_buf: [PAGE_SIZE]u8 = undefined;
                _ = try self.file.preadAll(&page_buf, page_num * PAGE_SIZE);
                var stream = std.io.fixedBufferStream(@as([]const u8, &page_buf));
                try page.deserialize(&stream);
            }
            self.cache[page_num] = page;

            if (page_num >= self.pages) {
                self.pages = page_num + 1;
            }
        }

        return page;
    }
};

pub const Node = struct {
    header: NodeHeader,
    body: NodeBody,

    const Self = @This();

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.SerializeError!void {
        try self.header.serialize(stream);
        try self.body.serialize(stream);
    }

    pub fn deserialize(self: *Self, stream: *std.io.FixedBufferStream([]const u8)) errors.DeserializeError!void {
        try self.header.deserialize(stream);
        try self.body.deserialize(stream);
    }
};

pub const NodeHeader = struct {
    parent: u32,
    is_root: u8,

    pub const SERIALIZED_SIZE =
        meta.sizeOfField(Self, .parent) +
        meta.sizeOfField(Self, .is_root) +
        @sizeOf(NodeType);

    const Self = @This();

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.SerializeError!void {
        var writer = stream.writer();
        try writer.writeInt(u8, self.is_root, .Little);
        try writer.writeInt(u32, self.parent, .Little);
    }

    pub fn deserialize(self: *Self, stream: *std.io.FixedBufferStream([]const u8)) errors.DeserializeError!void {
        var reader = stream.reader();
        self.is_root = try reader.readInt(u8, .Little);
        self.parent = try reader.readInt(u32, .Little);
    }
};

pub const NodeType = enum(u8) { Leaf };

pub const NodeBody = union(NodeType) {
    Leaf: LeafNode,

    const Self = @This();

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.SerializeError!void {
        var writer = stream.writer();
        switch (self.*) {
            .Leaf => {
                try writer.writeInt(u8, @enumToInt(NodeType.Leaf), .Little);
                try self.Leaf.serialize(stream);
            },
        }
    }

    pub fn deserialize(self: *Self, stream: *std.io.FixedBufferStream([]const u8)) errors.DeserializeError!void {
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

pub const LeafNodeHeader = struct {
    num_cells: u32,

    pub const SERIALIZED_SIZE = meta.sizeOfField(Self, .num_cells);

    const Self = @This();

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.SerializeError!void {
        var writer = stream.writer();
        try writer.writeInt(u32, self.num_cells, .Little);
    }

    pub fn deserialize(self: *Self, stream: *std.io.FixedBufferStream([]const u8)) errors.DeserializeError!void {
        var reader = stream.reader();
        self.num_cells = try reader.readInt(u32, .Little);
    }
};

pub const LeafNode = struct {
    header: LeafNodeHeader,
    cells: [MAX_CELLS]Cell,

    pub const SPACE_FOR_CELLS =
        Pager.PAGE_SIZE -
        NodeHeader.SERIALIZED_SIZE -
        LeafNodeHeader.SERIALIZED_SIZE;

    pub const MAX_CELLS = SPACE_FOR_CELLS / Cell.SERIALIZED_SIZE;

    const Self = @This();

    pub fn new() Self {
        var cells: [MAX_CELLS]Cell = undefined;
        std.mem.set(Cell, &cells, std.mem.zeroInit(Cell, .{}));
        return .{
            .header = .{ .num_cells = 0 },
            .cells = cells,
        };
    }

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.SerializeError!void {
        try self.header.serialize(stream);
        var cell_index: u32 = 0;
        while (cell_index < self.header.num_cells) : (cell_index += 1) {
            try self.cells[cell_index].serialize(stream);
        }
    }

    pub fn deserialize(self: *Self, stream: *std.io.FixedBufferStream([]const u8)) errors.DeserializeError!void {
        try self.header.deserialize(stream);
        var cell_index: u32 = 0;
        while (cell_index < self.header.num_cells) : (cell_index += 1) {
            try self.cells[cell_index].deserialize(stream);
        }
    }
};

pub const Cell = struct {
    key: u32,
    val: Row,

    pub const SERIALIZED_SIZE = meta.sizeOfField(Self, .key) + Row.SERIALIZED_SIZE;

    const Self = @This();

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.SerializeError!void {
        var writer = stream.writer();
        try writer.writeInt(u32, self.key, .Little);
        try self.val.serialize(stream);
    }

    pub fn deserialize(self: *Self, stream: *std.io.FixedBufferStream([]const u8)) errors.DeserializeError!void {
        var reader = stream.reader();
        self.key = try reader.readInt(u32, .Little);
        try self.val.deserialize(stream);
    }
};
