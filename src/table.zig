const std = @import("std");
const errors = @import("errors.zig");
const meta = @import("meta.zig");

const Pager = @import("pager.zig").Pager;
const Node = @import("pager.zig").Node;
const NodeHeader = @import("pager.zig").NodeHeader;
const LeafNode = @import("pager.zig").LeafNode;
const LeafNodeCell = @import("pager.zig").LeafNodeCell;
const InternalNode = @import("pager.zig").InternalNode;

/// A table is a collection of persistent rows. Data is stored in a B+ tree structure
/// for fast lookups.
pub const Table = struct {
    pager: Pager,
    root_page: u32,

    /// Error that occurs when working with a table.
    pub const Error = error{
        TableFull,
        DuplicateKey,
    } || Pager.Error || Row.Error;

    const Self = @This();

    /// Initialize a new table backed by the given allocator and the file at the given path.
    pub fn init(allocator: std.mem.Allocator, path: []const u8) Error!Self {
        var pager = try Pager.init(allocator, path);
        // If the file is empty, the data is newly created.
        if (pager.pages_len == 0) {
            var root = try pager.getPage(0);
            root.* = Node.new(.Leaf, 1, 0);
        }
        return Table{
            .pager = pager,
            .root_page = 0,
        };
    }

    /// Deinitialize the table by flusing all pages into disk and deallocating any allocated memory.
    pub fn deinit(self: *Self) void {
        var page_num: u32 = 0;
        while (page_num < self.pager.pages_len) : (page_num += 1) {
            // Flush any non-null page cache and deallocate its memory.
            if (self.pager.pages[page_num]) |page| {
                self.pager.flush(page_num) catch |err| {
                    std.log.err("Failed to flush page {d}: {!}", .{ page_num, err });
                };
                self.pager.allocator.destroy(page);
                self.pager.pages[page_num] = null;
            }
        }

        // Release all resources held by the pager.
        self.pager.deinit();
    }

    /// Insert a new row into the table. Changes are not persisted until the page is flushed.
    pub fn insert(self: *Self, row: *const Row) Error!void {
        var cursor = try self.find(self.root_page, row.id);
        const page = try self.pager.getPage(cursor.page);
        const leaf = &page.body.Leaf;

        if (cursor.cell < leaf.num_cells) {
            if (leaf.cells[cursor.cell].key == row.id) {
                return Error.DuplicateKey;
            }
        }
        try cursor.leafInsert(row.id, row);
    }

    /// Find all rows from the table and return an owned slice containing their data.
    pub fn select(self: *Self, allocator: std.mem.Allocator) Error![]Row {
        var rows = std.ArrayList(Row).init(allocator);
        var cursor = try self.head();
        while (!cursor.end) {
            const row_slot = try cursor.value_view();
            try rows.append(row_slot.*);
            try cursor.advance();
        }
        return rows.toOwnedSlice();
    }

    /// Get the cursor to the first row in the table.
    pub fn head(self: *Self) Error!Cursor {
        const cursor = try self.find(self.root_page, 0);
        const page = try self.pager.getPage(cursor.page);
        const leaf = &page.body.Leaf;
        return .{
            .table = self,
            .page = cursor.page,
            .cell = 0,
            .end = leaf.num_cells == 0,
        };
    }

    fn find(self: *Self, page_num: u32, key: u32) Error!Cursor {
        const page = try self.pager.getPage(page_num);
        switch (page.body) {
            .Leaf => |*leaf| {
                const index = leaf.find(key);
                return .{
                    .table = self,
                    .page = page_num,
                    .cell = index,
                    .end = index + 1 >= leaf.num_cells,
                };
            },
            .Internal => |internal| {
                const child_index = internal.find(key);
                const child = internal.getChild(child_index);
                return self.find(child, key);
            },
        }
    }

    fn createNewRoot(self: *Self, r_child_page_num: u32, r_child_page: *Node) Error!void {
        const root_page = try self.pager.getPage(self.root_page);
        const root_page_internal = &root_page.body.Internal;
        const l_child_page_num = self.pager.getUnusedPage();
        const l_child_page = try self.pager.getPage(l_child_page_num);

        l_child_page.* = root_page.*;
        l_child_page.header.is_root = 0;
        l_child_page.header.parent = self.root_page;
        r_child_page.header.is_root = 0;
        r_child_page.header.parent = self.root_page;

        root_page.* = Node.new(.Internal, 1, 0);
        root_page.header.is_root = 1;
        root_page_internal.num_keys = 1;
        root_page_internal.right_child = @intCast(u32, r_child_page_num);
        root_page_internal.cells[0].child = @intCast(u32, l_child_page_num);
        root_page_internal.cells[0].key = l_child_page.getMaxKey();
    }

    fn leafInsert(self: *Self, header: *const NodeHeader, leaf: *LeafNode, cell_num: u32, key: u32, val: *const Row) Error!void {
        if (leaf.num_cells >= LeafNode.MAX_CELLS) {
            return self.leafSplitInsert(header, leaf, cell_num, key, val);
        }

        if (cell_num < leaf.num_cells) {
            var idx = leaf.num_cells;
            while (idx > cell_num) : (idx -= 1) {
                leaf.cells[idx] = leaf.cells[idx - 1];
            }
        }

        leaf.num_cells += 1;
        leaf.cells[cell_num] = .{ .key = key, .val = val.* };
    }

    fn leafSplitInsert(self: *Self, header: *const NodeHeader, old_leaf: *LeafNode, cell_num: u32, key: u32, val: *const Row) Error!void {
        const new_page_num = self.pager.getUnusedPage();
        const new_page = try self.pager.getPage(new_page_num);
        new_page.* = Node.new(.Leaf, 0, 0);
        new_page.header.parent = header.parent;

        const new_leaf = &new_page.body.Leaf;
        new_leaf.num_cells = LeafNode.R_SPLIT_CELLS;
        new_leaf.next_leaf = old_leaf.next_leaf;

        old_leaf.num_cells = LeafNode.L_SPLIT_CELLS;
        old_leaf.next_leaf = @intCast(u32, new_page_num);

        const old_max = old_leaf.getMaxKey();
        var i: u32 = LeafNode.MAX_CELLS + 1;
        while (i > 0) : (i -= 1) {
            const old_cell_id = i - 1;
            var cells: *[LeafNode.MAX_CELLS]LeafNodeCell = undefined;
            if (old_cell_id >= LeafNode.L_SPLIT_CELLS) {
                cells = &new_leaf.cells;
            } else {
                cells = &old_leaf.cells;
            }

            const new_cell_id = old_cell_id % LeafNode.L_SPLIT_CELLS;
            if (old_cell_id == cell_num) {
                cells[new_cell_id] = .{ .key = key, .val = val.* };
            } else if (old_cell_id > cell_num) {
                cells[new_cell_id] = old_leaf.cells[old_cell_id - 1];
            } else {
                cells[new_cell_id] = old_leaf.cells[old_cell_id];
            }
        }

        if (header.is_root > 0) {
            try self.createNewRoot(new_page_num, new_page);
        } else {
            const new_max = old_leaf.getMaxKey();
            const parent_page = try self.pager.getPage(header.parent);
            var parent_internal = &parent_page.body.Internal;
            if (parent_internal.num_keys >= InternalNode.MAX_KEYS) {
                unreachable;
            }
            parent_internal.updateKey(old_max, new_max);
            try self.internalInsert(parent_internal, new_page_num, new_page);
        }
    }

    fn internalInsert(self: *Self, internal: *InternalNode, child_page_num: u32, child_page: *const Node) Error!void {
        const old_num_keys = internal.num_keys;
        internal.num_keys += 1;

        const parent_right_child_page_num = internal.right_child;
        const parent_right_child_page = try self.pager.getPage(parent_right_child_page_num);
        const parent_right_child_max_key = parent_right_child_page.getMaxKey();

        const child_max_key = child_page.getMaxKey();
        if (child_max_key > parent_right_child_max_key) {
            internal.cells[old_num_keys].key = parent_right_child_max_key;
            internal.cells[old_num_keys].child = parent_right_child_page_num;
            internal.right_child = child_page_num;
        } else {
            const child_internal = &child_page.body.Internal;
            const child_max_key_idx = child_internal.find(child_max_key);
            var i = old_num_keys;
            while (i > child_max_key_idx) : (i -= 1) {
                internal.cells[i] = internal.cells[i - 1];
            }
            internal.cells[i].child = child_page_num;
            internal.cells[i].key = child_max_key;
        }
    }
};

pub const Row = struct {
    id: u32,
    key_len: u8,
    val_len: u8,
    key_buf: [MAX_KEY_LEN]u8,
    val_buf: [MAX_VAL_LEN]u8,

    pub const MAX_KEY_LEN = (1 << 5);
    pub const MAX_VAL_LEN = (1 << 8) - 1;

    pub const SERIALIZED_SIZE =
        meta.sizeOfField(Self, .id) +
        meta.sizeOfField(Self, .key_len) +
        meta.sizeOfField(Self, .val_len) +
        meta.sizeOfField(Self, .key_buf) +
        meta.sizeOfField(Self, .val_buf);

    pub const Error = error{ KeyTooLong, ValueTooLong };

    const Self = @This();

    pub fn new(id: u32, key: []const u8, val: []const u8) Error!Self {
        if (key.len > MAX_KEY_LEN) return Error.KeyTooLong;
        if (val.len > MAX_VAL_LEN) return Error.ValueTooLong;
        var row = Self{
            .id = id,
            .key_len = @intCast(u8, key.len),
            .val_len = @intCast(u8, val.len),
            .key_buf = undefined,
            .val_buf = undefined,
        };
        std.mem.copy(u8, &row.key_buf, key);
        std.mem.copy(u8, &row.val_buf, val);
        return row;
    }

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.SerializeError!void {
        var writer = stream.writer();
        try writer.writeInt(u32, self.id, .Little);
        try writer.writeInt(u8, self.key_len, .Little);
        try writer.writeInt(u8, self.val_len, .Little);
        try writer.writeAll(&self.key_buf);
        try writer.writeAll(&self.val_buf);
    }

    pub fn deserialize(self: *Self, stream: *std.io.FixedBufferStream([]const u8)) errors.DeserializeError!void {
        var reader = stream.reader();
        self.id = try reader.readInt(u32, .Little);
        self.key_len = try reader.readInt(u8, .Little);
        self.val_len = try reader.readInt(u8, .Little);
        try reader.readNoEof(&self.key_buf);
        try reader.readNoEof(&self.val_buf);
    }
};

pub const Cursor = struct {
    table: *Table,
    page: u32,
    cell: u32,
    end: bool,

    pub const Error = Table.Error || Pager.Error;

    const Self = @This();

    pub fn value(self: *const Self) Error!*Row {
        const page = try self.table.pager.getPage(self.page);
        const leaf = &page.body.Leaf;
        return &leaf.cells[self.cell].val;
    }

    pub fn value_view(self: *const Self) Error!*const Row {
        return self.value();
    }

    pub fn advance(self: *Self) Error!void {
        const page = try self.table.pager.getPage(self.page);
        const leaf = &page.body.Leaf;
        self.cell += 1;
        if (self.cell >= leaf.num_cells) {
            if (leaf.next_leaf == 0) {
                self.end = true;
            } else {
                self.page = leaf.next_leaf;
                self.cell = 0;
            }
        }
    }

    pub fn leafInsert(self: *Self, key: u32, val: *const Row) Error!void {
        const page = try self.table.pager.getPage(self.page);
        try self.table.leafInsert(&page.header, &page.body.Leaf, self.cell, key, val);
    }

    pub fn leafSplitInsert(self: *Self, key: u32, val: *const Row) Error!void {
        const page = try self.table.pager.getPage(self.page);
        try self.table.leafSplitInsert(&page.header, &page.body.Leaf, self.cell, key, val);
    }
};
