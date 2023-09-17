const std = @import("std");
const squeal_pager = @import("pager.zig");

const Pager = squeal_pager.Pager(Row, PAGE_SIZE, PAGE_COUNT);
const Node = squeal_pager.Node(Row, PAGE_SIZE);
const NodeHeader = squeal_pager.NodeHeader;
const NodeLeaf = squeal_pager.NodeLeaf(Row, PAGE_SIZE);
const InternalNode = squeal_pager.NodeInternal(PAGE_SIZE);
const NodeCell = squeal_pager.NodeCell;

const PAGE_SIZE = 4096;
const PAGE_COUNT = 128;

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

    /// Initialize a new table backed by the given allocator and the file at the given path.
    pub fn init(allocator: std.mem.Allocator, path: []const u8) Error!@This() {
        var pager = try Pager.init(allocator, path);
        if (pager.page_count == 0) {
            // The file is newly created so we initialize a new table by making a new root node.
            var root = try pager.get(0);
            root.* = Node{
                .header = NodeHeader{
                    .parent = 0,
                    .is_root = true,
                    .is_leaf = true,
                },
                .body = undefined,
            };
        }
        return Table{
            .pager = pager,
            .root_page = 0,
        };
    }

    /// Deinitialize the table by flusing all pages into disk and deallocating any allocated memory.
    pub fn deinit(this: *@This()) void {
        var page: u32 = 0;
        while (page < this.pager.page_count) : (page += 1) {
            if (this.pager.page_cache[page]) |page_node| {
                // Flush any non-null page cache and deallocate its memory.
                this.pager.flush(page) catch |err| {
                    std.log.err("Failed to flush page {d}: {!}", .{ page, err });
                };
                this.pager.allocator.destroy(page_node);
                this.pager.page_cache[page] = null;
            }
        }
        // Release all resources held by the pager.
        this.pager.deinit();
    }

    /// Insert a new row into the table. Changes are not persisted until the page is flushed.
    pub fn insert(this: *@This(), row: *const Row) Error!void {
        // Find the leaf page where the row should be inserted.
        var cursor = try this.find(this.root_page, row.id);
        const page_node = try this.pager.get(cursor.page);
        const leaf = &page_node.body.leaf;
        if (cursor.cell < leaf.num_cells and leaf.cells[cursor.cell].key == row.id) {
            // The previously allocated row has the same id as the new row.
            return Error.DuplicateKey;
        }
        try cursor.insert(row.id, row);
    }

    /// Find all rows from the table and return an owned slice containing their data.
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
        const cursor = try this.find(this.root_page, 0);
        const page_node = try this.pager.get(cursor.page);
        const leaf = &page_node.body.leaf;
        return .{
            .table = this,
            .page = cursor.page,
            .cell = 0,
            .end = leaf.num_cells == 0,
        };
    }

    /// Get a cursor to a row in the table.
    fn find(this: *@This(), page: u32, key: u32) Error!Cursor {
        const node = try this.pager.get(page);
        if (node.header.is_leaf) {
            const leaf = &node.body.leaf;
            const index = leaf.find(key);
            return .{
                .table = this,
                .page = page,
                .cell = index,
                .end = index + 1 >= leaf.num_cells,
            };
        }
        const internal = &node.body.internal;
        const child_index = internal.find(key);
        const child = internal.getChild(child_index);
        return this.find(child, key);
    }

    /// This function is called after a root node was splitted into two nodes, the root can be either a leaf node
    /// or an internal node.
    ///
    /// ## Arguments
    ///
    /// - `root`: The current root node.
    /// - `key`: The key to be inserted into the new root.
    /// - `rnode`: The right child node of the new root.
    /// - `rnode_page`: The page number of the right child node.
    fn createNewRoot(this: *@This(), root: *Node, key: u32, rnode: *Node, rnode_page: u32) Error!void {
        // Allocate a new page for the left node of the new root and copy the current root
        // into it. The current root page contains the left node resulted from a previous split.
        const lnode_page = this.pager.getFree();
        const lnode = try this.pager.get(lnode_page);
        lnode.* = root.*;

        lnode.header.is_root = false;
        lnode.header.parent = this.root_page;
        rnode.header.is_root = false;
        rnode.header.parent = this.root_page;

        // Initialize a new internal node at the root page pointing the left and right child.
        const new_root = Node{
            .header = .{
                .parent = 0,
                .is_root = true,
                .is_leaf = false,
            },
            .body = undefined,
        };
        const new_root_body = &root.body.internal;
        new_root_body.num_keys = 1;
        new_root_body.right_child = @intCast(rnode_page);
        new_root_body.cells[0].val = @intCast(lnode_page);
        new_root_body.cells[0].key = key;
        root.* = new_root;

        // Update children the left node if it's an internal node. The children of the right node
        // should have their parent pointers updated by the previous routine(s).
        if (!lnode.header.is_leaf) {
            for (&lnode.body.internal.cells) |*cell| {
                const lnode_child = try this.pager.get(cell.val);
                lnode_child.header.parent = lnode_page;
            }
        }
    }

    /// This function is called to insert a new cell into the given leaf node.
    ///
    /// ## Arguments
    ///
    /// - `node`: The leaf node.
    /// - `cell`: The cell id to insert the new cell.
    /// - `key`: The key of the new cell.
    /// - `val`: The value of the new cell.
    fn leafInsert(this: *@This(), node: *Node, cell: u32, key: u32, val: *const Row) Error!void {
        const node_body = &node.body.leaf;
        if (node_body.num_cells >= NodeLeaf.MAX_CELLS) {
            // Leaf is full so we split.
            try this.leafSplitInsert(node, cell, key, val);
        } else {
            // copy cells after the insertion point backward one slot, leaving room at cell.
            if (cell < node_body.num_cells) {
                var idx = node_body.num_cells;
                while (idx > cell) : (idx -= 1) {
                    node_body.cells[idx] = node_body.cells[idx - 1];
                }
            }
            node_body.num_cells += 1;
            node_body.cells[cell] = .{ .key = key, .val = val.* };
        }
    }

    /// This function is called to insert a new cell into the given full leaf node.
    /// The function will split the leaf to make space for the new cell and update
    /// the tree accordingly.
    ///
    /// ## Arguments
    ///
    /// - `lnode`: The leaf node to be splitted.
    /// - `cell`: The cell id to insert the new cell.
    /// - `key`: The key of the new cell.
    /// - `val`: The value of the new cell.
    fn leafSplitInsert(this: *@This(), lnode: *Node, cell: u32, key: u32, val: *const Row) Error!void {
        const rnode_page = this.pager.getFree();
        const rnode = try this.pager.get(rnode_page);
        rnode.* = Node{
            .header = .{
                .parent = lnode.header.parent,
                .is_root = false,
                .is_leaf = true,
            },
            .body = undefined,
        };

        const lnode_body = &lnode.body.leaf;
        const rnode_body = &rnode.body.leaf;
        const lnode_max_key_old = try this.getTreeMaxKey(lnode);

        // Copy the cells of the left node into the right node while adding the new cell.
        var idx: u32 = NodeLeaf.MAX_CELLS + 1;
        while (idx > 0) : (idx -= 1) {
            const old_cell_id = idx - 1;
            var cells: *[NodeLeaf.MAX_CELLS]NodeCell(Row) = undefined;
            if (old_cell_id >= NodeLeaf.L_SPLIT_CELLS) {
                cells = &rnode_body.cells;
            } else {
                cells = &lnode_body.cells;
            }

            const new_cell_id = old_cell_id % NodeLeaf.L_SPLIT_CELLS;
            if (old_cell_id == cell) {
                cells[new_cell_id] = .{ .key = key, .val = val.* };
            } else if (old_cell_id > cell) {
                cells[new_cell_id] = lnode_body.cells[old_cell_id - 1];
            } else {
                cells[new_cell_id] = lnode_body.cells[old_cell_id];
            }
        }

        rnode_body.num_cells = NodeLeaf.R_SPLIT_CELLS;
        lnode_body.num_cells = NodeLeaf.L_SPLIT_CELLS;
        rnode_body.next_leaf = lnode_body.next_leaf;
        lnode_body.next_leaf = rnode_page;

        const lnode_max_key_new = try this.getTreeMaxKey(lnode);
        if (lnode.header.is_root) {
            try this.createNewRoot(lnode, lnode_max_key_new, rnode, rnode_page);
        } else {
            const rnode_max_key = try this.getTreeMaxKey(rnode);
            const parent = try this.pager.get(lnode.header.parent);
            try this.internalInsert(parent, rnode_max_key, rnode_page, lnode_max_key_old, lnode_max_key_new);
        }
    }

    /// This function is called to insert a new cell into the given internal node.
    ///
    /// ## Arguments
    ///
    /// - `page_node`: The internal node.
    /// - `new_node_key`: The key of the new cell
    /// - `new_node_page`: The child of the new cell.
    /// - `lnode_max_key_old`: The old key value of the cell
    /// - `lnode_max_key_new`: The new key value of the cell
    fn internalInsert(this: *@This(), node: *Node, new_node_key: u32, new_node_page: u32, lnode_max_key_old: u32, lnode_max_key_new: u32) Error!void {
        const node_body = &node.body.internal;

        // Because we just performed a split at 1 tree level bellow, the max key of node might
        // have change so we update it here.
        node_body.updateKey(lnode_max_key_old, lnode_max_key_new);

        if (node_body.num_keys >= InternalNode.MAX_KEYS) {
            try this.internalSplitInsert(node, new_node_key, new_node_page);
        } else {
            const rchild = try this.pager.get(node_body.right_child);
            const rchild_max_key = try this.getTreeMaxKey(rchild);
            if (new_node_key > rchild_max_key) {
                // The previous right child now is included in a cell in the node.
                node_body.cells[node_body.num_keys].key = rchild_max_key;
                node_body.cells[node_body.num_keys].val = node_body.right_child;
                // The insert key is larger than the largest key in the subtree, thus the page
                // become the new right child of the node.
                node_body.right_child = new_node_page;
            } else {
                // Move cells one position to the right to make space for the new cell.
                const new_node_key_idx = node_body.find(new_node_key);
                var cell_idx = node_body.num_keys;
                while (cell_idx > new_node_key_idx) : (cell_idx -= 1) {
                    node_body.cells[cell_idx] = node_body.cells[cell_idx - 1];
                }
                node_body.cells[cell_idx].val = new_node_page;
                node_body.cells[cell_idx].key = new_node_key;
            }
            node_body.num_keys += 1;
        }
    }

    /// This function is called to insert a new cell into the given full internal node.
    /// The function will split the internal to make space for the new cell and update
    /// the tree accordingly.
    ///
    /// ## Arguments
    ///
    /// - `page_node`: The internal node.
    /// - `new_node_key`: The key of the new cell
    /// - `new_node_page`: The child of the new cell.
    fn internalSplitInsert(this: *@This(), lnode: *Node, new_node_key: u32, new_node_page: u32) Error!void {
        const rnode_page = this.pager.getFree();
        const rnode = try this.pager.get(rnode_page);
        rnode.* = Node{
            .header = .{
                .parent = lnode.header.parent,
                .is_root = false,
                .is_leaf = false,
            },
            .body = undefined,
        };

        const lnode_body = &lnode.body.internal;
        const rnode_body = &rnode.body.internal;
        const lnode_max_key_old = try this.getTreeMaxKey(lnode);
        const new_node_cell_id = lnode_body.find(new_node_key);

        // Copy the cells of the left child into the right child while adding the new cell.
        var idx: u32 = InternalNode.MAX_KEYS + 1;
        while (idx > 0) : (idx -= 1) {
            const old_cell_id = idx - 1;
            var cells: *[InternalNode.MAX_KEYS]NodeCell(u32) = undefined;
            if (old_cell_id >= InternalNode.L_SPLIT_KEYS) {
                cells = &rnode_body.cells;
            } else {
                cells = &lnode_body.cells;
            }

            const new_cell_id = old_cell_id % InternalNode.L_SPLIT_KEYS;
            if (old_cell_id == new_node_cell_id) {
                cells[new_cell_id] = .{ .val = new_node_page, .key = new_node_key };
            } else if (old_cell_id > new_node_cell_id) {
                cells[new_cell_id] = lnode_body.cells[old_cell_id - 1];
            } else {
                cells[new_cell_id] = lnode_body.cells[old_cell_id];
            }
        }

        // We move the max key of the left child up to the parent. As a result, the left child contains
        // InternalNode.L_SPLIT_KEYS - 1 keys and the right child contains InternalNode.R_SPLIT_KEYS keys.
        lnode_body.num_keys = InternalNode.L_SPLIT_KEYS - 1;
        rnode_body.num_keys = InternalNode.R_SPLIT_KEYS;

        if (new_node_key > lnode_max_key_old) {
            const rnode_new_child_cell_id = new_node_cell_id % InternalNode.L_SPLIT_KEYS;
            // The new child is the new right child of the right internal node
            rnode_body.right_child = new_node_page;
            // Use the previous right child key and pointer for the new cell
            rnode_body.cells[rnode_new_child_cell_id].val = lnode_body.right_child;
            rnode_body.cells[rnode_new_child_cell_id].key = lnode_max_key_old;
        } else {
            // Move the right child over to our new right internal node
            rnode_body.right_child = lnode_body.right_child;
        }
        // Update the right child pointer be the child of the last cell of the left internal node
        lnode_body.right_child = lnode_body.cells[InternalNode.L_SPLIT_KEYS - 1].val;

        // Update the parent pointers of the children of the right internal node.
        const rnode_rchild = try this.pager.get(rnode_body.right_child);
        rnode_rchild.header.parent = rnode_page;
        for (rnode_body.cells) |cell| {
            const rchild_child = try this.pager.get(cell.val);
            rchild_child.header.parent = rnode_page;
        }

        const lnode_max_key_new = try this.getTreeMaxKey(lnode);
        if (lnode.header.is_root) {
            try this.createNewRoot(lnode, lnode_max_key_new, rnode, rnode_page);
        } else {
            const rchild_tree_max_key = try this.getTreeMaxKey(rnode);
            const parent = try this.pager.get(lnode.header.parent);
            try this.internalInsert(parent, rchild_tree_max_key, rnode_page, lnode_max_key_old, lnode_max_key_new);
        }
    }

    // Get the max key value of the tree rooted at the given page.
    fn getTreeMaxKey(this: *@This(), page: *Node) Error!u32 {
        if (page.header.is_leaf) {
            const leaf = &page.body.leaf;
            return leaf.cells[leaf.num_cells - 1].key;
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
        const leaf = &page.body.leaf;
        return &leaf.cells[this.cell].val;
    }

    /// Get a const reference the row that the cursor is pointing to.
    pub fn value_view(this: *const @This()) Error!*const Row {
        return this.value();
    }

    /// Advance the cursor to the next row.
    pub fn advance(this: *@This()) Error!void {
        const page = try this.table.pager.get(this.page);
        const leaf = &page.body.leaf;
        this.cell += 1;
        if (this.cell >= leaf.num_cells) {
            // Page 0 is reserved for the root page so we use the value 0
            // to encode the end of table because leaf node can't point to
            // the root node.
            if (leaf.next_leaf == 0) {
                this.end = true;
            } else {
                this.page = leaf.next_leaf;
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
