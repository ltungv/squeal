const std = @import("std");
const squeal_pager = @import("pager.zig");

pub const PAGE_SIZE = 32 * 1024;
pub const PAGE_COUNT = 1024 * 1024;

pub const Node = squeal_pager.Node(Row, PAGE_SIZE);
pub const NodeType = squeal_pager.NodeType;
pub const NodeHeader = squeal_pager.NodeHeader;
pub const NodeLeaf = squeal_pager.NodeLeaf(Row, PAGE_SIZE);
pub const NodeInternal = squeal_pager.NodeInternal(PAGE_SIZE);
pub const NodeCell = squeal_pager.NodeCell;
pub const Pager = squeal_pager.Pager(Row, PAGE_SIZE, PAGE_COUNT);

/// A row in the data table.
pub const Row = extern struct {
    id: u64,
    key_len: u8,
    val_len: u8,
    key_buf: [MAX_KEY_LEN]u8,
    val_buf: [MAX_VAL_LEN]u8,

    /// Max length of a key.
    pub const MAX_KEY_LEN = (1 << 8) - 1;
    /// Max length of a value.
    pub const MAX_VAL_LEN = (1 << 8) - 1;

    /// Row's error.
    pub const Error = error{ KeyTooLong, ValueTooLong };

    /// Create a new row.
    pub fn new(id: u64, key: []const u8, val: []const u8) Error!@This() {
        if (key.len > MAX_KEY_LEN) return Error.KeyTooLong;
        if (val.len > MAX_VAL_LEN) return Error.ValueTooLong;
        var key_buf: [MAX_KEY_LEN]u8 = undefined;
        var val_buf: [MAX_VAL_LEN]u8 = undefined;
        std.mem.copy(u8, &key_buf, key);
        std.mem.copy(u8, &val_buf, val);
        return .{
            .id = id,
            .key_len = @intCast(key.len),
            .val_len = @intCast(val.len),
            .key_buf = key_buf,
            .val_buf = val_buf,
        };
    }
};

/// A table is a collection of persistent rows stored in a B+ tree structure
/// for fast lookups. Rows are stored in pages, which are managed by a pager
/// that caches pages and flushes them to disk when needed.
pub const Table = struct {
    pager: *Pager,
    root_page: u64,

    /// Error that occurs when working with a table.
    pub const Error = error{
        TableFull,
        DuplicateKey,
    } || Pager.Error || Row.Error;

    /// Initialize a new table backed by the given pager.
    pub fn init(pager: *Pager) Error!@This() {
        if (pager.page_count == 0) {
            var root = try pager.get(0);
            root.* = Node{
                .header = NodeHeader{
                    .parent = 0,
                    .is_root = true,
                    .type = NodeType.Leaf,
                },
                .body = undefined,
            };
        }
        return Table{ .pager = pager, .root_page = 0 };
    }

    /// Deinitialize the table by flushing all pages into disk.
    pub fn deinit(this: *@This()) Error!void {
        try this.pager.flushAll();
    }

    /// Insert a new row into the table. Changes are not persisted until the
    /// page is flushed.
    pub fn insert(this: *@This(), row: Row) Error!void {
        try this.pager.clean();
        var cursor = try this.find(this.root_page, row.id);
        const page = try this.pager.get(cursor.page);
        if (cursor.cell < page.body.leaf.num_cells and
            page.body.leaf.cells[cursor.cell].key == row.id)
        {
            return Error.DuplicateKey;
        }
        try cursor.insert(row.id, row);
    }

    /// Find all rows from the table and return an owned slice containing
    /// their data.
    pub fn select(this: *@This(), allocator: std.mem.Allocator) Error![]Row {
        try this.pager.clean();
        var rows = std.ArrayList(Row).init(allocator);
        var cursor = try this.head();
        while (!cursor.end) {
            // Copy each row and put it in the ArrayList.
            const row_slot = try cursor.value_view();
            try rows.append(row_slot.*);
            try cursor.advance();
        }
        return rows.toOwnedSlice();
    }

    /// Get a cursor to the first row in the table.
    pub fn head(this: *@This()) Error!Cursor {
        // Find with key == 0 ensured that cursor return the cell
        // having the lowest key which also contains the first row
        // of the table.
        return this.find(this.root_page, 0);
    }

    /// Get a cursor to a row in the table.
    fn find(this: *@This(), page_num: u64, key: u64) Error!Cursor {
        const page = try this.pager.get(page_num);
        switch (page.header.type) {
            .Leaf => {
                const index = page.body.leaf.find(key);
                return .{
                    .table = this,
                    .page = page_num,
                    .cell = index,
                    .end = index >= page.body.leaf.num_cells,
                };
            },
            .Internal => {
                const index = page.body.internal.find(key);
                const child = page.body.internal.getChild(index);
                return this.find(child, key);
            },
        }
    }

    /// This function is called after a root node was splitted into two nodes,
    /// the root can be either a leaf node or an internal node.
    ///
    /// ## Arguments
    ///
    /// - `root`: The current root node.
    /// - `key`: The key to be inserted into the new root.
    /// - `rnode`: The right child node of the new root.
    /// - `rnode_page`: The page number of the right child node.
    fn createNewRoot(this: *@This(), root: *Node, key: u64, rnode: *Node, rnode_page: u64) Error!void {
        // Allocate a new page for the left node of the new root, then copy the
        // current root into the left node. The current root page contains the
        // left node resulted from a previous split.
        const lnode_page = this.pager.getFree();
        const lnode = try this.pager.get(lnode_page);
        lnode.* = root.*;
        // Update the root of each child.
        lnode.header.is_root = false;
        lnode.header.parent = this.root_page;
        rnode.header.is_root = false;
        rnode.header.parent = this.root_page;
        // Initialize a new root node.
        root.* = .{
            .header = .{ .parent = 0, .is_root = true, .type = NodeType.Internal },
            .body = .{
                .internal = .{ .num_keys = 1, .right_child = @intCast(rnode_page), .cells = undefined },
            },
        };
        root.body.internal.cells[0].key = key;
        root.body.internal.cells[0].val = @intCast(lnode_page);
        // Update children the left node if it's an internal node. The children
        // of the right node should have their parent pointers updated by the
        // previous routine(s).
        if (lnode.header.type == .Internal) {
            for (lnode.body.internal.cells[0..lnode.body.internal.num_keys]) |index| {
                const lnode_child = try this.pager.get(index.val);
                lnode_child.header.parent = lnode_page;
            }
        }
    }

    /// This function is called to insert a new cell into the given leaf node.
    /// This function assume the node body union is of a leaf node. Thus, the
    /// caller must check the node type before calling this function.
    ///
    /// ## Arguments
    ///
    /// - `node`: The leaf node.
    /// - `cell`: The cell insert position.
    /// - `key`: The key of the new cell.
    /// - `val`: The value of the new cell.
    fn leafInsert(this: *@This(), node: *Node, cell: u64, key: u64, val: Row) Error!void {
        if (node.body.leaf.num_cells >= NodeLeaf.MAX_CELLS) {
            // Leaf is full so we split.
            return this.leafSplitInsert(node, cell, key, val);
        }
        if (cell < node.body.leaf.num_cells) {
            // Shift all cells after the insertion point 1 slot towards the
            // end, leaving room for the new cell.
            var idx = node.body.leaf.num_cells;
            while (idx > cell) : (idx -= 1) {
                node.body.leaf.cells[idx] = node.body.leaf.cells[idx - 1];
            }
        }
        node.body.leaf.num_cells += 1;
        node.body.leaf.cells[cell] = .{ .key = key, .val = val };
    }

    /// This function is called to insert a new cell into the given full leaf
    /// node. The function will split the leaf to make space for the new cell
    /// and update the tree accordingly. This function assume the node body
    /// union is of a leaf node. Thus, the caller must check the node type
    /// before calling this function.
    ///
    /// ## Arguments
    ///
    /// - `lnode`: The leaf node to be splitted.
    /// - `cell`: The cell id to insert the new cell.
    /// - `key`: The key of the new cell.
    /// - `val`: The value of the new cell.
    fn leafSplitInsert(this: *@This(), lnode: *Node, cell: u64, key: u64, val: Row) Error!void {
        // Allocate a new leaf node for the right child.
        const rnode_page = this.pager.getFree();
        const rnode = try this.pager.get(rnode_page);
        rnode.* = .{
            .header = .{ .parent = lnode.header.parent, .is_root = false, .type = NodeType.Leaf },
            .body = .{ .leaf = undefined },
        };
        // Insert the new cell while splitting the node evenly. We track the
        // max key of the left node before and after the split to update the
        // key in case it changes due to the split.
        const lnode_max_key_old = try this.getTreeMaxKey(lnode);
        splitInsert(Row, &lnode.body.leaf.cells, &rnode.body.leaf.cells, NodeLeaf.L_SPLIT_CELLS, cell, key, val);
        rnode.body.leaf.num_cells = NodeLeaf.R_SPLIT_CELLS;
        rnode.body.leaf.next_leaf = lnode.body.leaf.next_leaf;
        lnode.body.leaf.num_cells = NodeLeaf.L_SPLIT_CELLS;
        lnode.body.leaf.next_leaf = rnode_page;
        // Propagate changes to upper levels.
        const lnode_max_key_new = try this.getTreeMaxKey(lnode);
        if (lnode.header.is_root) {
            // Create a new root using the max key of the left node.
            return this.createNewRoot(lnode, lnode_max_key_new, rnode, rnode_page);
        }
        // Insert the max key of the right node into the parent node.
        const rnode_max_key = try this.getTreeMaxKey(rnode);
        const parent = try this.pager.get(lnode.header.parent);
        try this.internalInsert(parent, rnode_max_key, rnode_page, lnode_max_key_old, lnode_max_key_new);
    }

    /// This function is called to insert a new cell into the given internal
    /// node. This function assume the node body union is of an internal node.
    /// Thus, the caller must check the node type before calling this function.
    ///
    ///
    /// ## Arguments
    ///
    /// - `page_node`: The internal node.
    /// - `new_node_key`: The key of the new cell
    /// - `new_node_page`: The child of the new cell.
    /// - `lnode_max_key_old`: The old key value of the cell
    /// - `lnode_max_key_new`: The new key value of the cell
    fn internalInsert(this: *@This(), node: *Node, new_node_key: u64, new_node_page: u64, lnode_max_key_old: u64, lnode_max_key_new: u64) Error!void {
        // Because we just performed a split at 1 tree level bellow, the max
        // key of node might have change so we update it here.
        node.body.internal.updateKey(lnode_max_key_old, lnode_max_key_new);
        if (node.body.internal.num_keys >= NodeInternal.MAX_KEYS) {
            return this.internalSplitInsert(node, new_node_key, new_node_page);
        }
        const rchild = try this.pager.get(node.body.internal.right_child);
        const rchild_max_key = try this.getTreeMaxKey(rchild);
        if (new_node_key > rchild_max_key) {
            // The insert key is larger than the largest key in the subtree,
            // thus the page become the new right child of the node. The
            // previous right child is now included in a cell.
            node.body.internal.cells[node.body.internal.num_keys].key = rchild_max_key;
            node.body.internal.cells[node.body.internal.num_keys].val = node.body.internal.right_child;
            node.body.internal.right_child = new_node_page;
        } else {
            // Shift all cells after the insertion point 1 slot towards the
            // end, leaving room for the new cell.
            const new_cell = node.body.internal.find(new_node_key);
            var cell = node.body.internal.num_keys;
            while (cell > new_cell) : (cell -= 1) {
                node.body.internal.cells[cell] = node.body.internal.cells[cell - 1];
            }
            node.body.internal.cells[cell].key = new_node_key;
            node.body.internal.cells[cell].val = new_node_page;
        }
        node.body.internal.num_keys += 1;
    }

    /// This function is called to insert a new cell into the given full
    /// internal node. The function will split the node to make space for the
    /// new cell and update the tree accordingly. This function assume the node
    /// body union is of an internal node. Thus, the caller must check the node
    /// type before calling this function.
    ///
    /// ## Arguments
    ///
    /// - `page_node`: The internal node.
    /// - `new_node_key`: The key of the new cell
    /// - `new_node_page`: The child of the new cell.
    fn internalSplitInsert(this: *@This(), lnode: *Node, key: u64, page: u64) Error!void {
        // Allocate the new right child.
        const rnode_page = this.pager.getFree();
        const rnode = try this.pager.get(rnode_page);
        rnode.* = .{
            .header = .{
                .parent = lnode.header.parent,
                .is_root = false,
                .type = NodeType.Internal,
            },
            .body = .{ .internal = undefined },
        };
        // Insert the new cell while splitting the node evenly. We track the
        // max key of the left node before and after the split to update the
        // key in case it changes due to the split.
        const lnode_max_key_old = try this.getTreeMaxKey(lnode);
        const cell = lnode.body.internal.find(key);
        splitInsert(u64, &lnode.body.internal.cells, &rnode.body.internal.cells, NodeInternal.L_SPLIT_KEYS, cell, key, page);
        lnode.body.internal.num_keys = NodeInternal.L_SPLIT_KEYS - 1;
        rnode.body.internal.num_keys = NodeInternal.R_SPLIT_KEYS;
        // Update the left internal node's right child pointer be the child of
        // its last cell. At this point, the right child of the left node has
        // been moved to the right node. Therefore, we need to assign a new
        // right child for it. Later, the max key of the left node will be
        // added to the upper levels. As a result, the left child only contains
        // InternalNode.L_SPLIT_KEYS - 1 keys.
        lnode.body.internal.right_child = lnode.body.internal.cells[NodeInternal.L_SPLIT_KEYS - 1].val;
        if (key > lnode_max_key_old) {
            // The new key is larger the max key of the left node before the
            // split. Thus, the inserted page becomes the right child of the
            // right node, and the previous right child is now included in a
            // cell whose key is the max key of the left node before the split.
            const rnode_cell = cell % NodeInternal.L_SPLIT_KEYS;
            rnode.body.internal.cells[rnode_cell].key = lnode_max_key_old;
            rnode.body.internal.cells[rnode_cell].val = lnode.body.internal.right_child;
            rnode.body.internal.right_child = page;
        } else {
            // The new key is smaller than the max key of the left node before
            // the split. Thus, the right child of the right node is the right
            // child of the left node before the split.
            rnode.body.internal.right_child = lnode.body.internal.right_child;
        }
        // Update the parent pointers of the children of the right internal
        // node. At this point, all the children are pointing to the left node.
        const rnode_rchild = try this.pager.get(rnode.body.internal.right_child);
        rnode_rchild.header.parent = rnode_page;
        for (rnode.body.internal.cells[0..rnode.body.internal.num_keys]) |index| {
            const rchild_child = try this.pager.get(index.val);
            rchild_child.header.parent = rnode_page;
        }
        // Propagate changes to upper levels.
        const lnode_max_key_new = try this.getTreeMaxKey(lnode);
        if (lnode.header.is_root) {
            // Create a new root using the max key of the left node.
            return this.createNewRoot(lnode, lnode_max_key_new, rnode, rnode_page);
        }
        // Insert the max key of the right node into the parent node.
        const rchild_tree_max_key = try this.getTreeMaxKey(rnode);
        const parent = try this.pager.get(lnode.header.parent);
        try this.internalInsert(parent, rchild_tree_max_key, rnode_page, lnode_max_key_old, lnode_max_key_new);
    }

    /// Get the max key value of the tree rooted at the given page.
    fn getTreeMaxKey(this: *@This(), page: *Node) Error!u64 {
        if (page.header.type == .Leaf) {
            // Max key of a leaf node is the key of its last cell.
            return page.body.leaf.cells[page.body.leaf.num_cells - 1].key;
        }
        // Max key of an internal node is the max key of its right child.
        const right_child = try this.pager.get(page.body.internal.right_child);
        return this.getTreeMaxKey(right_child);
    }
};

/// The cursor points to the location of a row within a leaf node.
/// When creating a cursor, we must make sure that `page` points to
/// a leaf node.
pub const Cursor = struct {
    table: *Table,
    page: u64,
    cell: u64,
    end: bool,

    pub const Error = Table.Error;

    /// Get a reference the row that the cursor is pointing to.
    pub fn value(this: *const @This()) Error!*Row {
        const page = try this.table.pager.get(this.page);
        return &page.body.leaf.cells[this.cell].val;
    }

    /// Get a const reference the row that the cursor is pointing to.
    pub fn value_view(this: *const @This()) Error!*const Row {
        return this.value();
    }

    /// Advance the cursor to the next row.
    pub fn advance(this: *@This()) Error!void {
        const page = try this.table.pager.get(this.page);
        this.cell += 1;
        if (this.cell >= page.body.leaf.num_cells) {
            // Page 0 is reserved for the root page so we use the value 0
            // to encode the end of table because leaf node can't point to
            // the root node.
            if (page.body.leaf.next_leaf == 0) {
                this.end = true;
            } else {
                this.page = page.body.leaf.next_leaf;
                this.cell = 0;
            }
        }
    }

    /// Insert a row into the leaf node.
    pub fn insert(this: *@This(), key: u64, val: Row) Error!void {
        var page = try this.table.pager.get(this.page);
        try this.table.leafInsert(page, this.cell, key, val);
    }
};

/// Insert into a full list of cells by splitting it into two before inserting.
/// After splitting, the first half of the original list remains while the last
/// half is copied into another list.
///
/// ## Arguments
///
/// - `T`: The type of the value stored in the cells.
/// - `lcells`: The left cells list (contains the original list before the split,
/// and contains the first half of it after the split).
/// - `rcells`: The right cells list (contains the last half of the original
/// list after the split).
/// - `left_cells_size`: The size of the left cells list after the split.
/// - `max_cells_size`: The maximum size of the cells list.
/// - `cell`: The index of the cell to insert in the original list.
/// - `key`: The key of the cell to insert.
/// - `value`: The value of the cell to insert.
fn splitInsert(comptime T: type, lcells: []NodeCell(T), rcells: []NodeCell(T), split_at: u64, cell: u64, key: u64, value: T) void {
    var idx = lcells.len + 1;
    while (idx > 0) : (idx -= 1) {
        var cells: []NodeCell(T) = undefined;
        const old_cell = idx - 1;
        if (old_cell >= split_at) {
            cells = rcells;
        } else {
            cells = lcells;
        }
        const new_cell = old_cell % split_at;
        if (old_cell == cell) {
            // Copy the the new cell.
            cells[new_cell] = .{ .key = key, .val = value };
        } else if (old_cell > cell) {
            // Cells come after the insertion point are shifted 1 slot
            // towards the end.
            cells[new_cell] = lcells[old_cell - 1];
        } else {
            // Cells come before the insertion point are copied as is
            cells[new_cell] = lcells[old_cell];
        }
    }
}
