const std = @import("std");
const squeal_pager = @import("pager.zig");

const PAGE_SIZE = 4096;
const PAGE_COUNT = 128;

const Node = squeal_pager.Node(Row, PAGE_SIZE);
const NodeType = squeal_pager.NodeType;
const NodeHeader = squeal_pager.NodeHeader;
const NodeLeaf = squeal_pager.NodeLeaf(Row, PAGE_SIZE);
const NodeInternal = squeal_pager.NodeInternal(PAGE_SIZE);
const NodeCell = squeal_pager.NodeCell;

pub const Pager = squeal_pager.Pager(Row, PAGE_SIZE, PAGE_COUNT);

/// A table is a collection of persistent rows stored in a B+ tree structure
/// for fast lookups. Rows are stored in pages, which are managed by a pager
/// that caches pages and flushes them to disk when needed.
pub const Table = struct {
    pager: *Pager,
    root_page: u32,

    /// Error that occurs when working with a table.
    pub const Error = error{
        TableFull,
        DuplicateKey,
    } || Pager.Error || Row.Error;

    /// Initialize a new table backed by the given allocator and the file at
    /// the given path.
    pub fn init(pager: *Pager) Error!@This() {
        if (pager.page_count == 0) {
            // Initialize a fresh table.
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

    /// Deinitialize the table by flusing all pages into disk and deallocating
    /// any allocated memory.
    pub fn deinit(this: *@This()) void {
        var page_num: u32 = 0;
        while (page_num < this.pager.page_count) : (page_num += 1) {
            // Flush any non-null page cache and deallocate its memory.
            const page = this.pager.page_cache[page_num] orelse continue;
            this.pager.flush(page_num) catch |err| {
                std.log.err("Failed to flush page {d}: {!}", .{ page_num, err });
            };
            this.pager.page_cache[page_num] = null;
            this.pager.allocator.destroy(page);
        }
    }

    /// Insert a new row into the table. Changes are not persisted until the
    /// page is flushed.
    pub fn insert(this: *@This(), row: *const Row) Error!void {
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
    fn find(this: *@This(), page_num: u32, key: u32) Error!Cursor {
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
    fn createNewRoot(this: *@This(), root: *Node, key: u32, rnode: *Node, rnode_page: u32) Error!void {
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
            .header = .{
                .parent = 0,
                .is_root = true,
                .type = NodeType.Internal,
            },
            .body = .{
                .internal = .{
                    .num_keys = 1,
                    .right_child = @intCast(rnode_page),
                    .cells = undefined,
                },
            },
        };
        root.body.internal.cells[0].val = @intCast(lnode_page);
        root.body.internal.cells[0].key = key;
        // Update children the left node if it's an internal node. The children
        // of the right node should have their parent pointers updated by the
        // previous routine(s).
        if (lnode.header.type == .Internal) {
            for (&lnode.body.internal.cells) |*cell| {
                const lnode_child = try this.pager.get(cell.val);
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
    fn leafInsert(this: *@This(), node: *Node, cell: u32, key: u32, val: *const Row) Error!void {
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
        node.body.leaf.cells[cell] = .{ .key = key, .val = val.* };
    }

    /// This function is called to insert a new cell into the given full leaf
    /// node. The function will split the leaf to make space for the new cell
    /// and update the tree accordingly.
    ///
    /// ## Arguments
    ///
    /// - `lnode`: The leaf node to be splitted.
    /// - `cell`: The cell id to insert the new cell.
    /// - `key`: The key of the new cell.
    /// - `val`: The value of the new cell.
    fn leafSplitInsert(this: *@This(), lnode: *Node, cell: u32, key: u32, val: *const Row) Error!void {
        // Allocate a new leaf node for the right child.
        const rnode_page = this.pager.getFree();
        const rnode = try this.pager.get(rnode_page);
        rnode.* = Node{
            .header = .{
                .parent = lnode.header.parent,
                .is_root = false,
                .type = NodeType.Leaf,
            },
            .body = undefined,
        };
        // Get largest key in the left node before the split.
        const lnode_max_key_old = try this.getTreeMaxKey(lnode);
        // Copy the cells of the left node into the right node while adding
        // the new cell.
        var idx: u32 = NodeLeaf.MAX_CELLS + 1;
        while (idx > 0) : (idx -= 1) {
            // Choose which array to copy the new into by comparing the current
            // index with the split point.
            var cells: *[NodeLeaf.MAX_CELLS]NodeCell(Row) = undefined;
            const old_cell_id = idx - 1;
            if (old_cell_id >= NodeLeaf.L_SPLIT_CELLS) {
                cells = &rnode.body.leaf.cells;
            } else {
                cells = &lnode.body.leaf.cells;
            }
            const new_cell_id = old_cell_id % NodeLeaf.L_SPLIT_CELLS;
            if (old_cell_id == cell) {
                // Copy the the new cell.
                cells[new_cell_id] = .{ .key = key, .val = val.* };
            } else if (old_cell_id > cell) {
                // Cells come after the insertion point are shifted 1 slot
                // towards the end.
                cells[new_cell_id] = lnode.body.leaf.cells[old_cell_id - 1];
            } else {
                // Cells come before the insertion point are copied as is
                cells[new_cell_id] = lnode.body.leaf.cells[old_cell_id];
            }
        }
        // Update the linked list of leaf nodes and the number of cells in each
        // child node.
        rnode.body.leaf.num_cells = NodeLeaf.R_SPLIT_CELLS;
        rnode.body.leaf.next_leaf = lnode.body.leaf.next_leaf;
        lnode.body.leaf.num_cells = NodeLeaf.L_SPLIT_CELLS;
        lnode.body.leaf.next_leaf = rnode_page;
        // Get largest key in the left node after the split.
        const lnode_max_key_new = try this.getTreeMaxKey(lnode);
        if (lnode.header.is_root) {
            // Create new root because we just split a root.
            return this.createNewRoot(lnode, lnode_max_key_new, rnode, rnode_page);
        }
        // Insert the new right node into the parent of the left node.
        const rnode_max_key = try this.getTreeMaxKey(rnode);
        const parent = try this.pager.get(lnode.header.parent);
        try this.internalInsert(parent, rnode_page, rnode_max_key, lnode_max_key_old, lnode_max_key_new);
    }

    /// This function is called to insert a new cell into the given internal
    /// node.
    ///
    /// ## Arguments
    ///
    /// - `page_node`: The internal node.
    /// - `new_node_key`: The key of the new cell
    /// - `new_node_page`: The child of the new cell.
    /// - `lnode_max_key_old`: The old key value of the cell
    /// - `lnode_max_key_new`: The new key value of the cell
    fn internalInsert(
        this: *@This(),
        node: *Node,
        new_node_page: u32,
        new_node_key: u32,
        lnode_max_key_old: u32,
        lnode_max_key_new: u32,
    ) Error!void {
        // Because we just performed a split at 1 tree level bellow, the max
        // key of node might have change so we update it here.
        node.body.internal.updateKey(lnode_max_key_old, lnode_max_key_new);
        if (node.body.internal.num_keys >= NodeInternal.MAX_KEYS) {
            return this.internalSplitInsert(node, new_node_page, new_node_key);
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
            // Move cells one position to the right to make space for the
            // new cell.
            const new_node_key_idx = node.body.internal.find(new_node_key);
            var cell_idx = node.body.internal.num_keys;
            while (cell_idx > new_node_key_idx) : (cell_idx -= 1) {
                node.body.internal.cells[cell_idx] = node.body.internal.cells[cell_idx - 1];
            }
            node.body.internal.cells[cell_idx].val = new_node_page;
            node.body.internal.cells[cell_idx].key = new_node_key;
        }
        node.body.internal.num_keys += 1;
    }

    /// This function is called to insert a new cell into the given full
    /// internal node. The function will split the node to make space for the
    /// new cell and update the tree accordingly.
    ///
    /// ## Arguments
    ///
    /// - `page_node`: The internal node.
    /// - `new_node_key`: The key of the new cell
    /// - `new_node_page`: The child of the new cell.
    fn internalSplitInsert(this: *@This(), lnode: *Node, new_node_page: u32, new_node_key: u32) Error!void {
        // Allocate the new right child.
        const rnode_page = this.pager.getFree();
        const rnode = try this.pager.get(rnode_page);
        rnode.* = Node{
            .header = .{
                .parent = lnode.header.parent,
                .is_root = false,
                .type = NodeType.Internal,
            },
            .body = undefined,
        };
        // Get largest key in the left node before the split.
        const lnode_max_key_old = try this.getTreeMaxKey(lnode);
        // Copy the cells of the left child into the right child while adding
        // the new cell.
        const new_node_cell_id = lnode.body.internal.find(new_node_key);
        var idx: u32 = NodeInternal.MAX_KEYS + 1;
        while (idx > 0) : (idx -= 1) {
            var cells: *[NodeInternal.MAX_KEYS]NodeCell(u32) = undefined;
            const old_cell_id = idx - 1;
            if (old_cell_id >= NodeInternal.L_SPLIT_KEYS) {
                cells = &rnode.body.internal.cells;
            } else {
                cells = &lnode.body.internal.cells;
            }
            const new_cell_id = old_cell_id % NodeInternal.L_SPLIT_KEYS;
            if (old_cell_id == new_node_cell_id) {
                cells[new_cell_id] = .{ .val = new_node_page, .key = new_node_key };
            } else if (old_cell_id > new_node_cell_id) {
                cells[new_cell_id] = lnode.body.internal.cells[old_cell_id - 1];
            } else {
                cells[new_cell_id] = lnode.body.internal.cells[old_cell_id];
            }
        }
        // We move the max key of the left child up to the parent. As a result,
        // the left child contains InternalNode.L_SPLIT_KEYS - 1 keys
        lnode.body.internal.num_keys = NodeInternal.L_SPLIT_KEYS - 1;
        rnode.body.internal.num_keys = NodeInternal.R_SPLIT_KEYS;
        if (new_node_key > lnode_max_key_old) {
            // The new child is the new right child of the right internal node
            rnode.body.internal.right_child = new_node_page;
            // Use the previous right child key and pointer for the new cell
            const rnode_new_child_cell_id = new_node_cell_id % NodeInternal.L_SPLIT_KEYS;
            rnode.body.internal.cells[rnode_new_child_cell_id].val = lnode.body.internal.right_child;
            rnode.body.internal.cells[rnode_new_child_cell_id].key = lnode_max_key_old;
        } else {
            // Move the right child over to our new right internal node
            rnode.body.internal.right_child = lnode.body.internal.right_child;
        }
        // Update the right child pointer be the child of the last cell of the
        // left internal node
        lnode.body.internal.right_child = lnode.body.internal.cells[NodeInternal.L_SPLIT_KEYS - 1].val;
        // Update the parent pointers of the children of the right internal node.
        const rnode_rchild = try this.pager.get(rnode.body.internal.right_child);
        rnode_rchild.header.parent = rnode_page;
        for (rnode.body.internal.cells) |cell| {
            const rchild_child = try this.pager.get(cell.val);
            rchild_child.header.parent = rnode_page;
        }
        // Get largest key in the left node before the split.
        const lnode_max_key_new = try this.getTreeMaxKey(lnode);
        if (lnode.header.is_root) {
            try this.createNewRoot(lnode, lnode_max_key_new, rnode, rnode_page);
        } else {
            const rchild_tree_max_key = try this.getTreeMaxKey(rnode);
            const parent = try this.pager.get(lnode.header.parent);
            try this.internalInsert(parent, rnode_page, rchild_tree_max_key, lnode_max_key_old, lnode_max_key_new);
        }
    }

    // Get the max key value of the tree rooted at the given page.
    fn getTreeMaxKey(this: *@This(), page: *Node) Error!u32 {
        if (page.header.type == .Leaf) {
            return page.body.leaf.cells[page.body.leaf.num_cells - 1].key;
        }
        const right_child = try this.pager.get(page.body.internal.right_child);
        return this.getTreeMaxKey(right_child);
    }
};

pub const Row = extern struct {
    id: u32,
    key_len: u8,
    val_len: u8,
    key_buf: [MAX_KEY_LEN]u8,
    val_buf: [MAX_VAL_LEN]u8,

    pub const MAX_KEY_LEN = (1 << 5);
    pub const MAX_VAL_LEN = (1 << 8) - 1;

    pub const Error = error{ KeyTooLong, ValueTooLong };

    pub fn new(id: u32, key: []const u8, val: []const u8) Error!@This() {
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

/// The cursor points to the location of a row within a leaf node.
/// When creating a cursor, we must make sure that `page` points to
/// a leaf node.
pub const Cursor = struct {
    table: *Table,
    page: u32,
    cell: u32,
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
    pub fn insert(this: *@This(), key: u32, val: *const Row) Error!void {
        var page = try this.table.pager.get(this.page);
        try this.table.leafInsert(page, this.cell, key, val);
    }
};
