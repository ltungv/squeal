const std = @import("std");
const errors = @import("errors.zig");
const meta = @import("meta.zig");

const Pager = @import("pager.zig").Pager;
const Node = @import("pager.zig").Node;
const NodeHeader = @import("pager.zig").NodeHeader;
const LeafNode = @import("pager.zig").LeafNode;
const LeafNodeCell = @import("pager.zig").LeafNodeCell;
const InternalNode = @import("pager.zig").InternalNode;
const InternalNodeCell = @import("pager.zig").InternalNodeCell;

/// A table is a collection of persistent rows stored in a B+ tree structure
/// for fast lookups. Rows are stored in pages, which are managed by a pager
/// that caches pages and flushes them to disk when needed.
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
        if (pager.pages_len == 0) {
            // The file is newly created so we initialize a new table by making a new root node.
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
        var page: u32 = 0;
        while (page < self.pager.pages_len) : (page += 1) {
            if (self.pager.pages[page]) |page_node| {
                // Flush any non-null page cache and deallocate its memory.
                self.pager.flush(page) catch |err| {
                    std.log.err("Failed to flush page {d}: {!}", .{ page, err });
                };
                self.pager.allocator.destroy(page_node);
                self.pager.pages[page] = null;
            }
        }
        // Release all resources held by the pager.
        self.pager.deinit();
    }

    /// Insert a new row into the table. Changes are not persisted until the page is flushed.
    pub fn insert(self: *Self, row: *const Row) Error!void {
        // Find the leaf page where the row should be inserted.
        var cursor = try self.find(self.root_page, row.id);
        const page_node = try self.pager.getPage(cursor.page);
        const leaf = &page_node.body.Leaf;
        if (cursor.cell < leaf.num_cells and leaf.cells[cursor.cell].key == row.id) {
            // The previously allocated row has the same id as the new row.
            return Error.DuplicateKey;
        }
        try cursor.leafInsert(row.id, row);
    }

    /// Find all rows from the table and return an owned slice containing their data.
    pub fn select(self: *Self, allocator: std.mem.Allocator) Error![]Row {
        var rows = std.ArrayList(Row).init(allocator);
        var cursor = try self.head();
        while (!cursor.end) {
            // Copy each row and put it in the ArrayList.
            const row_slot = try cursor.value_view();
            try rows.append(row_slot.*);
            try cursor.advance();
        }
        return rows.toOwnedSlice();
    }

    /// Get a cursor to the first row in the table.
    pub fn head(self: *Self) Error!Cursor {
        // Find with key == 0 ensured that cursor return the cell
        // having the lowest key which also contains the first row
        // of the table.
        const cursor = try self.find(self.root_page, 0);
        const page_node = try self.pager.getPage(cursor.page);
        const leaf = &page_node.body.Leaf;
        return .{
            .table = self,
            .page = cursor.page,
            .cell = 0,
            .end = leaf.num_cells == 0,
        };
    }

    /// Get a cursor to a row in the table.
    fn find(self: *Self, page: u32, key: u32) Error!Cursor {
        const p = try self.pager.getPage(page);
        switch (p.body) {
            .Leaf => |*leaf| {
                const index = leaf.find(key);
                return .{
                    .table = self,
                    .page = page,
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

    /// This function is called after a root node was splitted into two nodes, the root can be either a leaf node
    /// or an internal node.
    ///
    /// ## Arguments
    ///
    /// - `root`: The current root node.
    /// - `key`: The key to be inserted into the new root.
    /// - `rchild`: The right child node of the new root.
    /// - `rchild_page`: The page number of the right child node.
    fn createNewRoot(self: *Self, root: *Node, key: u32, rchild: *Node, rchild_page: u32) Error!void {
        // Allocate a new page for the left child of the new root and copy the current root
        // into it. The current root page contains the left child page resulted from a previous split.
        const lchild_page = self.pager.getUnusedPage();
        const lchild = try self.pager.getPage(lchild_page);
        lchild.* = root.*;

        lchild.header.is_root = 0;
        lchild.header.parent = self.root_page;
        rchild.header.is_root = 0;
        rchild.header.parent = self.root_page;

        // Initialize a new internal node at the root page pointing the left and right child.
        const new_root = Node.new(.Internal, 1, 0);
        const new_root_body = &root.body.Internal;
        new_root_body.num_keys = 1;
        new_root_body.right_child = @intCast(u32, rchild_page);
        new_root_body.cells[0].child = @intCast(u32, lchild_page);
        new_root_body.cells[0].key = key;
        root.* = new_root;

        // Update children the left child if it's an internal node. The children of the right child
        // should have their parent pointers updated by the previous routine(s).
        if (lchild.body == .Internal) {
            for (lchild.body.Internal.cells) |*cell| {
                const internal_child = try self.pager.getPage(cell.child);
                internal_child.header.parent = lchild_page;
            }
        }
    }

    /// This function is called to insert a new cell into the given leaf node.
    ///
    /// ## Arguments
    ///
    /// - `page_node`: The leaf node.
    /// - `cell`: The cell id to insert the new cell.
    /// - `key`: The key of the new cell.
    /// - `val`: The value of the new cell.
    fn leafInsert(self: *Self, page_node: *Node, cell: u32, key: u32, val: *const Row) Error!void {
        const page_body = &page_node.body.Leaf;
        if (page_body.num_cells >= LeafNode.MAX_CELLS) {
            // Leaf is full so we split.
            try self.leafSplitInsert(page_node, cell, key, val);
        } else {
            // copy cells after the insertion point backward one slot, leaving room at cell.
            if (cell < page_body.num_cells) {
                var idx = page_body.num_cells;
                while (idx > cell) : (idx -= 1) {
                    page_body.cells[idx] = page_body.cells[idx - 1];
                }
            }
            page_body.num_cells += 1;
            page_body.cells[cell] = .{ .key = key, .val = val.* };
        }
    }

    /// This function is called to insert a new cell into the given full leaf node.
    /// The function will split the leaf to make space for the new cell and update
    /// the tree accordingly.
    ///
    /// ## Arguments
    ///
    /// - `lchild`: The leaf node to be splitted.
    /// - `cell`: The cell id to insert the new cell.
    /// - `key`: The key of the new cell.
    /// - `val`: The value of the new cell.
    fn leafSplitInsert(self: *Self, lchild: *Node, cell: u32, key: u32, val: *const Row) Error!void {
        const rchild_page = self.pager.getUnusedPage();
        const rchild = try self.pager.getPage(rchild_page);
        rchild.* = Node.new(.Leaf, 0, lchild.header.parent);

        const lchild_body = &lchild.body.Leaf;
        const rchild_body = &rchild.body.Leaf;
        const lchild_max_key_old = try self.getTreeMaxKey(lchild);

        // Copy the cells of the left child into the right child while adding the new cell.
        var idx: u32 = LeafNode.MAX_CELLS + 1;
        while (idx > 0) : (idx -= 1) {
            const old_cell_id = idx - 1;
            var cells: *[LeafNode.MAX_CELLS]LeafNodeCell = undefined;
            if (old_cell_id >= LeafNode.L_SPLIT_CELLS) {
                cells = &rchild_body.cells;
            } else {
                cells = &lchild_body.cells;
            }

            const new_cell_id = old_cell_id % LeafNode.L_SPLIT_CELLS;
            if (old_cell_id == cell) {
                cells[new_cell_id] = .{ .key = key, .val = val.* };
            } else if (old_cell_id > cell) {
                cells[new_cell_id] = lchild_body.cells[old_cell_id - 1];
            } else {
                cells[new_cell_id] = lchild_body.cells[old_cell_id];
            }
        }

        rchild_body.num_cells = LeafNode.R_SPLIT_CELLS;
        lchild_body.num_cells = LeafNode.L_SPLIT_CELLS;
        rchild_body.next_leaf = lchild_body.next_leaf;
        lchild_body.next_leaf = rchild_page;

        const lchild_max_key_new = try self.getTreeMaxKey(lchild);
        if (lchild.header.is_root > 0) {
            try self.createNewRoot(lchild, lchild_max_key_new, rchild, rchild_page);
        } else {
            const rchild_max_key = try self.getTreeMaxKey(rchild);
            const parent = try self.pager.getPage(lchild.header.parent);
            try self.internalInsert(parent, rchild_max_key, rchild_page, lchild_max_key_old, lchild_max_key_new);
        }
    }

    /// This function is called to insert a new cell into the given internal node.
    ///
    /// ## Arguments
    ///
    /// - `page_node`: The internal node.
    /// - `new_child_key`: The key of the new cell
    /// - `new_child_page`: The child of the new cell.
    /// - `lchild_max_key_old`: The old key value of the cell
    /// - `lchild_max_key_new`: The new key value of the cell
    fn internalInsert(self: *Self, page_node: *Node, new_child_key: u32, new_child_page: u32, lchild_max_key_old: u32, lchild_max_key_new: u32) Error!void {
        const page_body = &page_node.body.Internal;

        // Because we just performed a split at 1 tree level bellow, the max key of page_node might
        // have change so we update it here.
        page_body.updateKey(lchild_max_key_old, lchild_max_key_new);

        if (page_body.num_keys >= InternalNode.MAX_KEYS) {
            try self.internalSplitInsert(page_node, new_child_key, new_child_page);
        } else {
            const rchild = try self.pager.getPage(page_body.right_child);
            const rchild_max_key = try self.getTreeMaxKey(rchild);
            if (new_child_key > rchild_max_key) {
                // The previous right child now is included in a cell in the node.
                page_body.cells[page_body.num_keys].key = rchild_max_key;
                page_body.cells[page_body.num_keys].child = page_body.right_child;
                // The insert key is larger than the largest key in the subtree, thus the page
                // become the new right child of the node.
                page_body.right_child = new_child_page;
            } else {
                // Move cells one position to the right to make space for the new cell.
                const new_key_idx = page_body.find(new_child_key);
                var cell_idx = page_body.num_keys;
                while (cell_idx > new_key_idx) : (cell_idx -= 1) {
                    page_body.cells[cell_idx] = page_body.cells[cell_idx - 1];
                }
                page_body.cells[cell_idx].child = new_child_page;
                page_body.cells[cell_idx].key = new_child_key;
            }
            page_body.num_keys += 1;
        }
    }

    /// This function is called to insert a new cell into the given full internal node.
    /// The function will split the internal to make space for the new cell and update
    /// the tree accordingly.
    ///
    /// ## Arguments
    ///
    /// - `page_node`: The internal node.
    /// - `new_child_key`: The key of the new cell
    /// - `new_child_page`: The child of the new cell.
    fn internalSplitInsert(self: *Self, lchild: *Node, new_child_key: u32, new_child_page: u32) Error!void {
        const rchild_page = self.pager.getUnusedPage();
        const rchild = try self.pager.getPage(rchild_page);
        rchild.* = Node.new(.Internal, 0, lchild.header.parent);

        const lchild_body = &lchild.body.Internal;
        const rchild_body = &rchild.body.Internal;
        const lchild_max_key_old = try self.getTreeMaxKey(lchild);
        const new_child_cell_id = lchild_body.find(new_child_key);

        // Copy the cells of the left child into the right child while adding the new cell.
        var idx: u32 = InternalNode.MAX_KEYS + 1;
        while (idx > 0) : (idx -= 1) {
            const old_cell_id = idx - 1;
            var cells: *[InternalNode.MAX_KEYS]InternalNodeCell = undefined;
            if (old_cell_id >= InternalNode.L_SPLIT_KEYS) {
                cells = &rchild_body.cells;
            } else {
                cells = &lchild_body.cells;
            }

            const new_cell_id = old_cell_id % InternalNode.L_SPLIT_KEYS;
            if (old_cell_id == new_child_cell_id) {
                cells[new_cell_id] = .{ .child = new_child_page, .key = new_child_key };
            } else if (old_cell_id > new_child_cell_id) {
                cells[new_cell_id] = lchild_body.cells[old_cell_id - 1];
            } else {
                cells[new_cell_id] = lchild_body.cells[old_cell_id];
            }
        }

        // We move the max key of the left child up to the parent. As a result, the left child contains
        // InternalNode.L_SPLIT_KEYS - 1 keys and the right child contains InternalNode.R_SPLIT_KEYS keys.
        lchild_body.num_keys = InternalNode.L_SPLIT_KEYS - 1;
        rchild_body.num_keys = InternalNode.R_SPLIT_KEYS;

        if (new_child_key > lchild_max_key_old) {
            const rchild_new_child_cell_id = new_child_cell_id % InternalNode.L_SPLIT_KEYS;
            // The new child is the new right child of the right internal node
            rchild_body.right_child = new_child_page;
            // Use the previous right child key and pointer for the new cell
            rchild_body.cells[rchild_new_child_cell_id].child = lchild_body.right_child;
            rchild_body.cells[rchild_new_child_cell_id].key = lchild_max_key_old;
        } else {
            // Move the right child over to our new right internal node
            rchild_body.right_child = lchild_body.right_child;
        }
        // Update the right child pointer be the child of the last cell of the left internal node
        lchild_body.right_child = lchild_body.cells[InternalNode.L_SPLIT_KEYS - 1].child;

        // Update the parent pointers of the children of the right internal node.
        const rinternal_rchild = try self.pager.getPage(rchild_body.right_child);
        rinternal_rchild.header.parent = rchild_page;
        for (rchild_body.cells) |cell| {
            const rchild_child = try self.pager.getPage(cell.child);
            rchild_child.header.parent = rchild_page;
        }

        const lchild_max_key_new = try self.getTreeMaxKey(lchild);
        if (lchild.header.is_root > 0) {
            try self.createNewRoot(lchild, lchild_max_key_new, rchild, rchild_page);
        } else {
            const rchild_tree_max_key = try self.getTreeMaxKey(rchild);
            const parent = try self.pager.getPage(lchild.header.parent);
            try self.internalInsert(parent, rchild_tree_max_key, rchild_page, lchild_max_key_old, lchild_max_key_new);
        }
    }

    // Get the max key value of the tree rooted at the given page.
    fn getTreeMaxKey(self: *Self, page: *Node) Error!u32 {
        switch (page.body) {
            .Leaf => |*leaf| return leaf.cells[leaf.num_cells - 1].key,
            .Internal => |*internal| {
                const right_child = try self.pager.getPage(internal.right_child);
                return self.getTreeMaxKey(right_child);
            },
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

/// The cursor points to the location of a row within a leaf node.
/// When creating a cursor, we must make sure that `page` points to
/// a leaf node.
pub const Cursor = struct {
    table: *Table,
    page: u32,
    cell: u32,
    end: bool,

    pub const Error = Table.Error;

    const Self = @This();

    /// Get a reference the row that the cursor is pointing to.
    pub fn value(self: *const Self) Error!*Row {
        const page = try self.table.pager.getPage(self.page);
        const leaf = &page.body.Leaf;
        return &leaf.cells[self.cell].val;
    }

    /// Get a const reference the row that the cursor is pointing to.
    pub fn value_view(self: *const Self) Error!*const Row {
        return self.value();
    }

    /// Advance the cursor to the next row.
    pub fn advance(self: *Self) Error!void {
        const page = try self.table.pager.getPage(self.page);
        const leaf = &page.body.Leaf;
        self.cell += 1;
        if (self.cell >= leaf.num_cells) {
            // Page 0 is reserved for the root page so we use the value 0
            // to encode the end of table because leaf node can't point to
            // the root node.
            if (leaf.next_leaf == 0) {
                self.end = true;
            } else {
                self.page = leaf.next_leaf;
                self.cell = 0;
            }
        }
    }

    /// Insert a row into the leaf node.
    pub fn leafInsert(self: *Self, key: u32, val: *const Row) Error!void {
        var page = try self.table.pager.getPage(self.page);
        try self.table.leafInsert(page, self.cell, key, val);
    }
};
