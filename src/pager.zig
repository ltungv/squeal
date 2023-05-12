const std = @import("std");
const errors = @import("errors.zig");
const meta = @import("meta.zig");

const Row = @import("table.zig").Row;
const Table = @import("table.zig").Table;

/// A pager is responsible for reading and writing pages (blocks of data) to a file.
/// Changes made on a page are not persisted until the page is flushed.
pub const Pager = struct {
    allocator: std.mem.Allocator,
    len: u32,
    file: std.fs.File,
    pages_len: u32,
    pages: [MAX_PAGES]?*Node,

    /// Size of each page in bytes.
    pub const PAGE_SIZE = 4096;

    /// Max number of allowed pages.
    pub const MAX_PAGES = 100;

    /// Error that occurs when using a pager.
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

    /// Create a new pager backed by the given allocator and file.
    pub fn init(allocator: std.mem.Allocator, path: []const u8) Error!Self {
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
        if (file_size % PAGE_SIZE != 0) {
            return Error.Corrupted;
        }

        // Initialize all cached pages to null.
        var pages: [MAX_PAGES]?*Node = undefined;
        std.mem.set(?*Node, &pages, null);

        return Self{
            .allocator = allocator,
            .len = file_size,
            .file = file,
            .pages_len = file_size / PAGE_SIZE,
            .pages = pages,
        };
    }

    /// Deinitialize the pager. This flushes all pages to disk and frees any allocated memory.
    pub fn deinit(self: *Self) void {
        for (self.pages) |*nullable_page| {
            if (nullable_page.*) |page| {
                self.allocator.destroy(page);
                nullable_page.* = null;
            }
        }
        self.file.close();
    }

    /// Flush a page to disk.
    pub fn flush(self: *Self, page: u32) Error!void {
        const page_node = self.pages[page] orelse return Error.NullPage;
        var buf: [PAGE_SIZE]u8 = undefined;
        var stream = std.io.fixedBufferStream(&buf);
        try page_node.serialize(&stream);
        _ = try self.file.pwriteAll(&buf, page * PAGE_SIZE);
    }

    /// Get a pointer to a cached page. If the page is not in cache, it will be read from disk.
    pub fn getPage(self: *Self, page: u32) Error!*Node {
        if (page >= MAX_PAGES) {
            return Error.OutOfBound;
        }

        var page_node: *Node = undefined;
        if (self.pages[page]) |node| {
            page_node = node;
        } else {
            page_node = try self.allocator.create(Node);
            const num_pages = self.len / PAGE_SIZE;
            if (page < num_pages) {
                // Load page from disk if it exists.
                var page_buf: [PAGE_SIZE]u8 = undefined;
                _ = try self.file.preadAll(&page_buf, page * PAGE_SIZE);
                // Deserialize page into its in-memory representation.
                var stream = std.io.fixedBufferStream(@as([]const u8, &page_buf));
                try page_node.deserialize(&stream);
            }

            if (page >= self.pages_len) {
                self.pages_len = page + 1;
            }
            self.pages[page] = page_node;
        }

        return page_node;
    }

    pub fn getUnusedPage(self: *const Self) u32 {
        return self.pages_len;
    }
};

/// A B+ tree node that can be either a leaf or an internal node.
/// Each node is a page in the file.
pub const Node = struct {
    header: NodeHeader,
    body: NodeBody,

    pub const SERIALIZED_LEAF_SIZE = NodeHeader.SERIALIZED_SIZE + NodeBody.SERIALIZED_LEAF_SIZE;
    pub const SERIALIZED_INTERNAL_SIZE = NodeHeader.SERIALIZED_SIZE + NodeBody.SERIALIZED_INTERNAL_SIZE;

    const Self = @This();

    pub fn new(node_type: NodeType, is_root: u8, parent: u32) Self {
        const header = NodeHeader{ .is_root = is_root, .parent = parent };
        const body = NodeBody.new(node_type);
        return .{ .header = header, .body = body };
    }

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

    pub const SERIALIZED_SIZE = meta.sizeOfField(Self, .parent) + meta.sizeOfField(Self, .is_root);

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

pub const NodeType = enum(u8) { Leaf, Internal };

pub const NodeBody = union(NodeType) {
    Leaf: LeafNode,
    Internal: InternalNode,

    pub const SERIALIZED_LEAF_SIZE = @sizeOf(NodeType) + LeafNode.SERIALIZED_SIZE;
    pub const SERIALIZED_INTERNAL_SIZE = @sizeOf(NodeType) + InternalNode.SERIALIZED_SIZE;

    const Self = @This();

    pub fn new(node_type: NodeType) Self {
        return switch (node_type) {
            .Leaf => .{ .Leaf = LeafNode.new() },
            .Internal => .{ .Internal = InternalNode.new() },
        };
    }

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.SerializeError!void {
        var writer = stream.writer();
        switch (self.*) {
            .Leaf => |leaf| {
                try writer.writeInt(u8, @enumToInt(NodeType.Leaf), .Little);
                try leaf.serialize(stream);
            },
            .Internal => |internal| {
                try writer.writeInt(u8, @enumToInt(NodeType.Internal), .Little);
                try internal.serialize(stream);
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
            .Internal => {
                var internal: InternalNode = undefined;
                try internal.deserialize(stream);
                self.* = .{ .Internal = internal };
            },
        }
    }
};

pub const LeafNode = struct {
    num_cells: u32,
    next_leaf: u32,
    cells: [MAX_CELLS]LeafNodeCell,

    pub const SPACE_FOR_CELLS =
        Pager.PAGE_SIZE -
        NodeHeader.SERIALIZED_SIZE -
        @sizeOf(NodeType) -
        @sizeOf(u32) * 2;

    pub const MAX_CELLS = SPACE_FOR_CELLS / LeafNodeCell.SERIALIZED_SIZE;
    pub const R_SPLIT_CELLS = (MAX_CELLS + 1) / 2;
    pub const L_SPLIT_CELLS = (MAX_CELLS + 1) - R_SPLIT_CELLS;

    pub const SERIALIZED_SIZE = @sizeOf(u32) * 2 + LeafNodeCell.SERIALIZED_SIZE * MAX_CELLS;

    const Self = @This();

    pub fn new() Self {
        return std.mem.zeroInit(Self, .{});
    }

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.SerializeError!void {
        var writer = stream.writer();
        try writer.writeInt(u32, self.num_cells, .Little);
        try writer.writeInt(u32, self.next_leaf, .Little);
        var cell_index: u32 = 0;
        while (cell_index < self.num_cells) : (cell_index += 1) {
            try self.cells[cell_index].serialize(stream);
        }
    }

    pub fn deserialize(self: *Self, stream: *std.io.FixedBufferStream([]const u8)) errors.DeserializeError!void {
        var reader = stream.reader();
        self.num_cells = try reader.readInt(u32, .Little);
        self.next_leaf = try reader.readInt(u32, .Little);
        var cell_index: u32 = 0;
        while (cell_index < self.num_cells) : (cell_index += 1) {
            try self.cells[cell_index].deserialize(stream);
        }
    }

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

pub const LeafNodeCell = struct {
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

pub const InternalNode = struct {
    num_keys: u32,
    right_child: u32,
    cells: [MAX_KEYS]InternalNodeCell,

    pub const SPACE_FOR_CELLS =
        Pager.PAGE_SIZE -
        NodeHeader.SERIALIZED_SIZE -
        @sizeOf(NodeType) -
        @sizeOf(u32) * 2;

    pub const MAX_KEYS = SPACE_FOR_CELLS / InternalNodeCell.SERIALIZED_SIZE;
    pub const R_SPLIT_KEYS = (MAX_KEYS + 1) / 2;
    pub const L_SPLIT_KEYS = (MAX_KEYS + 1) - R_SPLIT_KEYS;

    pub const SERIALIZED_SIZE = @sizeOf(u32) * 2 + InternalNodeCell.SERIALIZED_SIZE * MAX_KEYS;

    const Self = @This();

    pub fn new() Self {
        return std.mem.zeroInit(Self, .{});
    }

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.SerializeError!void {
        var writer = stream.writer();
        try writer.writeInt(u32, self.num_keys, .Little);
        try writer.writeInt(u32, self.right_child, .Little);
        var cell_index: u32 = 0;
        while (cell_index < self.num_keys) : (cell_index += 1) {
            try self.cells[cell_index].serialize(stream);
        }
    }

    pub fn deserialize(self: *Self, stream: *std.io.FixedBufferStream([]const u8)) errors.DeserializeError!void {
        var reader = stream.reader();
        self.num_keys = try reader.readInt(u32, .Little);
        self.right_child = try reader.readInt(u32, .Little);
        var cell_index: u32 = 0;
        while (cell_index < self.num_keys) : (cell_index += 1) {
            try self.cells[cell_index].deserialize(stream);
        }
    }

    pub fn getChild(self: *const Self, index: u32) u32 {
        if (index == self.num_keys) return self.right_child;
        return self.cells[index].child;
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

pub const InternalNodeCell = struct {
    child: u32,
    key: u32,

    pub const SERIALIZED_SIZE = meta.sizeOfField(Self, .key) + meta.sizeOfField(Self, .child);

    const Self = @This();

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.SerializeError!void {
        var writer = stream.writer();
        try writer.writeInt(u32, self.child, .Little);
        try writer.writeInt(u32, self.key, .Little);
    }

    pub fn deserialize(self: *Self, stream: *std.io.FixedBufferStream([]const u8)) errors.DeserializeError!void {
        var reader = stream.reader();
        self.child = try reader.readInt(u32, .Little);
        self.key = try reader.readInt(u32, .Little);
    }
};
