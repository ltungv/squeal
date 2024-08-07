const std = @import("std");
const squeal_assert = @import("assert.zig");
const squeal_pager = @import("pager.zig");

const TestTable = Table(Row, 4096, 64);

/// A table is a collection of persistent rows stored in a B+ tree structure
/// for fast lookups. Rows are stored in pages, which are managed by a pager
/// that caches pages and flushes them to disk when needed.
pub fn Table(comptime T: type, comptime PAGE_SIZE: u64, comptime PAGE_COUNT: u64) type {
    return struct {
        pager: *Pager,
        root_page: u64,

        pub const TreeNode = Node(T, PAGE_SIZE);
        pub const TreeLeaf = NodeLeaf(T, PAGE_SIZE);
        pub const TreeInternal = NodeInternal(PAGE_SIZE);
        pub const Pager = squeal_pager.Pager(TreeNode, PAGE_SIZE, PAGE_COUNT);

        /// Error that occurs when working with a table.
        pub const Error = error{ TableFull, DuplicateKey } || Pager.Error;

        /// The cursor points to the location of a row within a leaf node.
        /// When creating a cursor, we must make sure that `page` points to
        /// a leaf node.
        pub const Cursor = struct {
            table: *Table(T, PAGE_SIZE, PAGE_COUNT),
            page: u64,
            cell: u64,
            end: bool,

            /// Get a reference the row that the cursor is pointing to.
            pub fn value(this: *const @This()) Error!*T {
                const page = try this.table.pager.get(this.page);
                return &page.body.leaf.cells[this.cell].val;
            }

            /// Get a const reference the row that the cursor is pointing to.
            pub fn value_view(this: *const @This()) Error!*const T {
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
            pub fn insert(this: *@This(), key: u64, val: *const T) Error!void {
                const page = try this.table.pager.get(this.page);
                try this.table.leafInsert(page, this.cell, key, val);
            }
        };

        /// Initialize a new table backed by the given pager.
        pub fn init(pager: *Pager) Error!@This() {
            if (pager.page_count == 0) {
                const root = try pager.get(0);
                root.* = TreeNode.init(0, true, .Leaf);
            }
            return .{ .pager = pager, .root_page = 0 };
        }

        /// Deinitialize the table by flushing all pages into disk.
        pub fn deinit(this: *@This()) Error!void {
            try this.pager.flushAll();
        }

        /// Count the number of row currently in the table.
        pub fn count(this: *@This()) Error!u64 {
            const cursor = try this.head();
            var page = try this.pager.get(cursor.page);
            var rowCount = page.body.leaf.num_cells;
            while (page.body.leaf.next_leaf != 0) {
                page = try this.pager.get(page.body.leaf.next_leaf);
                rowCount += page.body.leaf.num_cells;
            }
            return rowCount;
        }

        /// Find all rows from the table and return an owned slice containing
        /// their data.
        pub fn select(this: *@This(), allocator: std.mem.Allocator) Error![]T {
            try this.pager.clean();
            var rows = std.ArrayList(T).init(allocator);
            var cursor = try this.head();
            while (!cursor.end) {
                const row_slot = try cursor.value_view();
                try rows.append(row_slot.*);
                try cursor.advance();
            }
            return rows.toOwnedSlice();
        }

        /// Insert a new row into the table. Changes are not persisted until the
        /// page is flushed.
        pub fn insert(this: *@This(), row: *const T) Error!void {
            try this.pager.clean();
            var cursor = try this.find(this.root_page, row.id);
            const page = try this.pager.get(cursor.page);
            if (cursor.cell < page.body.leaf.num_cells and
                page.body.leaf.cells[cursor.cell].key == row.id) return Error.DuplicateKey;
            try cursor.insert(row.id, row);
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
        fn createNewRoot(this: *@This(), root: *TreeNode, key: u64, rnode: *TreeNode, rnode_page: u64) Error!void {
            // Allocate a new page, then copy the current root into it. The current
            // root page contains the left node resulted from a previous split.
            const lnode_page = this.pager.getFree();
            const lnode = try this.pager.get(lnode_page);
            lnode.* = root.*;
            // Update the root of each child.
            lnode.header.is_root = false;
            lnode.header.parent = this.root_page;
            rnode.header.is_root = false;
            rnode.header.parent = this.root_page;
            // Initialize a new root node.
            root.* = TreeNode.init(0, true, .Internal);
            root.body.internal = .{ .num_keys = 1, .right_child = @intCast(rnode_page), .cells = undefined };
            root.body.internal.cells[0].key = key;
            root.body.internal.cells[0].val = @intCast(lnode_page);
            if (lnode.header.type == .Internal) try this.internalSetChildrenParent(lnode, lnode_page);
        }

        /// This function is called to insert a new cell into the given leaf node
        /// assuming the node is a leaf node. The caller must check the node type
        /// before calling this function.
        ///
        /// ## Arguments
        ///
        /// - `node`: The leaf node.
        /// - `cell`: The cell insert position.
        /// - `key`: The key of the new cell.
        /// - `val`: The value of the new cell.
        fn leafInsert(this: *@This(), node: *TreeNode, cell: u64, key: u64, val: *const T) Error!void {
            std.debug.assert(node.header.type == .Leaf);
            if (node.body.leaf.num_cells >= TreeLeaf.MAX_CELLS) return this.leafSplitInsert(node, cell, key, val);
            cellsInsert(T, &node.body.leaf.cells, node.body.leaf.num_cells, cell, key, val);
            node.body.leaf.num_cells += 1;
        }

        /// This function is called to insert a new cell into the given full leaf
        /// node assuming the node body is a leaf node. The caller must check the
        /// node type before calling this function. The leaf will be splitted to
        /// make space for the new cell when it's full.
        ///
        /// ## Arguments
        ///
        /// - `lnode`: The leaf node to be splitted.
        /// - `cell`: The cell id to insert the new cell.
        /// - `key`: The key of the new cell.
        /// - `val`: The value of the new cell.
        fn leafSplitInsert(this: *@This(), lnode: *TreeNode, cell: u64, key: u64, val: *const T) Error!void {
            std.debug.assert(lnode.header.type == .Leaf);
            const lnode_max_key_old = try this.getTreeMaxKey(lnode);
            const rnode_page = this.pager.getFree();
            const rnode = try this.pager.get(rnode_page);
            rnode.* = TreeNode.init(lnode.header.parent, false, .Leaf);
            cellsSplitInsert(T, &lnode.body.leaf.cells, &rnode.body.leaf.cells, TreeLeaf.L_SPLIT_CELLS, cell, key, val.*);
            rnode.body.leaf.num_cells = TreeLeaf.R_SPLIT_CELLS;
            rnode.body.leaf.next_leaf = lnode.body.leaf.next_leaf;
            lnode.body.leaf.num_cells = TreeLeaf.L_SPLIT_CELLS;
            lnode.body.leaf.next_leaf = rnode_page;
            // Create a new root.
            const lnode_max_key_new = try this.getTreeMaxKey(lnode);
            if (lnode.header.is_root) return this.createNewRoot(lnode, lnode_max_key_new, rnode, rnode_page);
            // Insert the max key of the right node into the parent node.
            const rnode_max_key = try this.getTreeMaxKey(rnode);
            const parent = try this.pager.get(lnode.header.parent);
            try this.internalInsert(parent, rnode_max_key, rnode_page, lnode_max_key_old, lnode_max_key_new);
        }

        /// This function is called to insert a new cell into the given internal
        /// node assuming the node body union is an internal node. The caller must
        /// check the node type before calling this function.
        ///
        /// ## Arguments
        ///
        /// - `node`: The internal node.
        /// - `key`: The key of the new cell
        /// - `page`: The child of the new cell.
        /// - `lnode_max_key_old`: The old key value of the cell.
        /// - `lnode_max_key_new`: The new key value of the cell.
        fn internalInsert(this: *@This(), node: *TreeNode, key: u64, page: u64, lnode_max_key_old: u64, lnode_max_key_new: u64) Error!void {
            std.debug.assert(node.header.type == .Internal);
            // We just performed a split at 1 tree level bellow, so the max key
            // might have change.
            node.body.internal.updateKey(lnode_max_key_old, lnode_max_key_new);
            if (node.body.internal.num_keys >= TreeInternal.MAX_KEYS) return this.internalSplitInsert(node, key, page);
            const rchild = try this.pager.get(node.body.internal.right_child);
            const rchild_max_key = try this.getTreeMaxKey(rchild);
            if (key > rchild_max_key) {
                // The insert key is larger than the largest key in the subtree,
                // thus the page become the new right child of the node. The
                // previous right child is now included in a cell.
                node.body.internal.cells[node.body.internal.num_keys].key = rchild_max_key;
                node.body.internal.cells[node.body.internal.num_keys].val = node.body.internal.right_child;
                node.body.internal.right_child = page;
            } else {
                const new_cell = node.body.internal.find(key);
                cellsInsert(u64, &node.body.internal.cells, node.body.internal.num_keys, new_cell, key, &page);
            }
            node.body.internal.num_keys += 1;
        }

        /// This function is called to insert a new cell into the given full
        /// internal node assuming the node body is an internal node. The caller
        /// must check the node type before calling this function. The node will
        /// be splitted to make space for the new cell when it's full.
        ///
        /// ## Arguments
        ///
        /// - `lnode`: The internal node.
        /// - `key`: The key of the new cell
        /// - `page`: The child of the new cell.
        fn internalSplitInsert(this: *@This(), lnode: *TreeNode, key: u64, page: u64) Error!void {
            std.debug.assert(lnode.header.type == .Internal);
            const cell = lnode.body.internal.find(key);
            const lnode_max_key_old = try this.getTreeMaxKey(lnode);
            const rnode_page = this.pager.getFree();
            const rnode = try this.pager.get(rnode_page);
            rnode.* = TreeNode.init(lnode.header.parent, false, .Internal);
            cellsSplitInsert(u64, &lnode.body.internal.cells, &rnode.body.internal.cells, TreeInternal.L_SPLIT_KEYS, cell, key, page);
            lnode.body.internal.num_keys = TreeInternal.L_SPLIT_KEYS - 1;
            rnode.body.internal.num_keys = TreeInternal.R_SPLIT_KEYS;
            if (key > lnode_max_key_old) {
                // The new key is larger the max key of the left node before the
                // split. Thus, the inserted page becomes the right child of the
                // right node, and the previous right child is now included in a
                // cell whose key is the max key of the left node before the split.
                const rnode_cell = cell % TreeInternal.L_SPLIT_KEYS;
                rnode.body.internal.cells[rnode_cell].key = lnode_max_key_old;
                rnode.body.internal.cells[rnode_cell].val = lnode.body.internal.right_child;
                rnode.body.internal.right_child = page;
            } else {
                // The new key is smaller than the max key of the left node before
                // the split. Thus, the right child of the right node is the right
                // child of the left node before the split.
                rnode.body.internal.right_child = lnode.body.internal.right_child;
            }
            // Update the left internal node's right child pointer be the child of
            // its last cell. At this point, the right child of the left node has
            // been moved to the right node. Therefore, we need to assign a new
            // right child for it. Later, the max key of the left node will be
            // added to the upper levels. As a result, the left child only contains
            // InternalNode.L_SPLIT_KEYS - 1 keys.
            lnode.body.internal.right_child = lnode.body.internal.cells[TreeInternal.L_SPLIT_KEYS - 1].val;
            // Update the parent pointers of the children of the right internal
            // node. At this point, all the children are pointing to the left node.
            try this.internalSetChildrenParent(rnode, rnode_page);
            // Create a new root using the max key of the left node.
            const lnode_max_key_new = try this.getTreeMaxKey(lnode);
            if (lnode.header.is_root) return this.createNewRoot(lnode, lnode_max_key_new, rnode, rnode_page);
            // Insert the max key of the right node into the parent node. We pass
            // the current and new max key of the left node, in case it was updated.
            const rchild_tree_max_key = try this.getTreeMaxKey(rnode);
            const parent = try this.pager.get(lnode.header.parent);
            try this.internalInsert(parent, rchild_tree_max_key, rnode_page, lnode_max_key_old, lnode_max_key_new);
        }

        fn internalSetChildrenParent(this: *@This(), node: *TreeNode, parent: u64) Error!void {
            std.debug.assert(node.header.type == .Internal);
            for (node.body.internal.cells[0..node.body.internal.num_keys]) |index| {
                const child = try this.pager.get(index.val);
                child.header.parent = parent;
            }
            const child = try this.pager.get(node.body.internal.right_child);
            child.header.parent = parent;
        }

        /// Get the max key value of the tree rooted at the given page.
        fn getTreeMaxKey(this: *@This(), page: *TreeNode) Error!u64 {
            switch (page.header.type) {
                .Leaf => return page.body.leaf.cells[page.body.leaf.num_cells - 1].key,
                .Internal => {
                    const right_child = try this.pager.get(page.body.internal.right_child);
                    return this.getTreeMaxKey(right_child);
                },
            }
        }

        /// Insert into a list of cells by shifting all cells after the insertion point
        /// 1 slot towards the end, leaving room for the new cell.
        ///
        /// ## Arguments
        ///
        /// - `T`: The type of the value stored in the cells.
        /// - `cells`: The list of cells.
        /// - `cells_len`: The length of the list of cells.
        /// - `new_cell`: The index of the new cell to insert.
        /// - `key`: The key of the new cell.
        /// - `value`: The value of the new cell.
        fn cellsInsert(
            comptime TCellValue: type,
            cells: []NodeCell(TCellValue),
            cells_len: u64,
            new_cell: u64,
            key: u64,
            value: *const TCellValue,
        ) void {
            var cell = cells_len;
            while (cell > new_cell) : (cell -= 1) cells[cell] = cells[cell - 1];
            cells[cell].key = key;
            cells[cell].val = value.*;
        }

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
        fn cellsSplitInsert(
            comptime TCellValue: type,
            lcells: []NodeCell(TCellValue),
            rcells: []NodeCell(TCellValue),
            split_at: u64,
            cell: u64,
            key: u64,
            value: TCellValue,
        ) void {
            var idx = lcells.len + 1;
            while (idx > 0) : (idx -= 1) {
                const old_cell = idx - 1;
                const new_cell = old_cell % split_at;
                var cells: []NodeCell(TCellValue) = undefined;
                if (old_cell >= split_at) cells = rcells else cells = lcells;
                if (old_cell < cell) {
                    // Cells come before the insertion point are copied as is
                    cells[new_cell] = lcells[old_cell];
                } else if (old_cell > cell) {
                    // Cells come after the insertion point are shifted 1 slot
                    // towards the end.
                    cells[new_cell] = lcells[old_cell - 1];
                } else {
                    // Copy the the new cell.
                    cells[new_cell] = .{ .key = key, .val = value };
                }
            }
        }
    };
}

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
        @memcpy(key_buf[0..key.len], key);
        @memcpy(val_buf[0..val.len], val);
        return .{
            .id = id,
            .key_len = @intCast(key.len),
            .val_len = @intCast(val.len),
            .key_buf = key_buf,
            .val_buf = val_buf,
        };
    }
};

/// A node within a B+ tree, which can be one of two types:
/// + Leaf node containing data entries and their keys.
/// + Internal node containing key indices and pointers to child node.
pub fn Node(comptime T: type, comptime PAGE_SIZE: usize) type {
    return extern struct {
        header: NodeHeader,
        body: NodeBody(T, PAGE_SIZE),

        /// Initialize a node.
        pub fn init(parent: u64, is_root: bool, node_type: NodeType) @This() {
            return .{
                .header = NodeHeader.init(parent, is_root, node_type),
                .body = NodeBody(T, PAGE_SIZE).init(node_type),
            };
        }
    };
}

/// Type of a node in a B+ tree.
pub const NodeType = enum(u8) {
    Leaf,
    Internal,
};

/// Header of a node in a B+ tree containing its metadata.
pub const NodeHeader = extern struct {
    parent: u64,
    is_root: bool,
    type: NodeType,

    /// Initialize a node header.
    pub fn init(parent: u64, is_root: bool, node_type: NodeType) @This() {
        return .{ .parent = parent, .is_root = is_root, .type = node_type };
    }
};

/// Body of a node in a B+ tree, which can be one of two types leaf or internal.
/// The node type is determined explicitly by the header insteading of using
/// Zig's tagged union.
pub fn NodeBody(comptime T: type, comptime PAGE_SIZE: usize) type {
    return extern union {
        leaf: NodeLeaf(T, PAGE_SIZE),
        internal: NodeInternal(PAGE_SIZE),

        /// Initialize a node body.
        pub fn init(node_type: NodeType) @This() {
            switch (node_type) {
                .Leaf => return .{ .leaf = NodeLeaf(T, PAGE_SIZE).init() },
                .Internal => return .{ .internal = NodeInternal(PAGE_SIZE).init() },
            }
        }
    };
}

/// Content of a leaf node in a B+ tree.
pub fn NodeLeaf(comptime T: type, comptime PAGE_SIZE: usize) type {
    return extern struct {
        next_leaf: u64,
        num_cells: u64,
        cells: [MAX_CELLS]NodeCell(T),

        /// Max number of data cells a leaf node can hold.
        pub const MAX_CELLS = (PAGE_SIZE - @sizeOf(NodeHeader) - @sizeOf(u64) * 2) / @sizeOf(NodeCell(T));
        /// Number of cells in the right leaf node after splitting.
        pub const R_SPLIT_CELLS = (MAX_CELLS + 1) / 2;
        /// Number of cells in the left leaf node after splitting.
        pub const L_SPLIT_CELLS = (MAX_CELLS + 1) - R_SPLIT_CELLS;

        /// Initialize a leaf node.
        pub fn init() @This() {
            return .{ .next_leaf = 0, .num_cells = 0, .cells = undefined };
        }

        /// Find the index of the cell with the given key using binary search.
        /// If there's no cell with the given key, an index of where the cell
        /// should be is returned.
        pub fn find(this: *const @This(), key: u64) u64 {
            return searchCells(T, this.cells[0..this.num_cells], key);
        }
    };
}

/// Content of an internal node in a B+ tree.
pub fn NodeInternal(comptime PAGE_SIZE: usize) type {
    return extern struct {
        right_child: u64,
        num_keys: u64,
        cells: [MAX_KEYS]NodeCell(u64),

        /// Max number of data cells an internal node can hold.
        pub const MAX_KEYS = (PAGE_SIZE - @sizeOf(NodeHeader) - @sizeOf(u64) * 2) / @sizeOf(NodeCell(u64));
        /// Number of cells in the right internal node after splitting.
        pub const R_SPLIT_KEYS = (MAX_KEYS + 1) / 2;
        /// Number of cells in the left internal node after splitting.
        pub const L_SPLIT_KEYS = (MAX_KEYS + 1) - R_SPLIT_KEYS;

        /// Initialize an internal node.
        pub fn init() @This() {
            return .{ .right_child = 0, .num_keys = 0, .cells = undefined };
        }

        /// Return the child node index at the given index.
        pub fn getChild(this: *const @This(), index: u64) u64 {
            if (index == this.num_keys) return this.right_child;
            return this.cells[index].val;
        }

        /// Find the index of the cell with the given key using binary search.
        /// If there's no cell with the given key, an index of where the cell
        /// should be is returned.
        pub fn find(this: *const @This(), key: u64) u64 {
            return searchCells(u64, this.cells[0..this.num_keys], key);
        }

        /// Find the index at old_key and update its value to new_key.
        pub fn updateKey(this: *@This(), old_key: u64, new_key: u64) void {
            const index = this.find(old_key);
            if (index < this.num_keys) this.cells[index].key = new_key;
        }
    };
}

/// A data cell within a node in a B+ tree.
pub fn NodeCell(comptime T: type) type {
    return extern struct {
        key: u64,
        val: T,
    };
}

fn searchCells(comptime T: type, cells: []const NodeCell(T), key: u64) u64 {
    var left: u64 = 0;
    var right: u64 = cells.len;
    while (left < right) {
        const index = (left + right) / 2;
        const cell = cells[index];
        if (key == cell.key) return index;
        if (key < cell.key) right = index else left = index + 1;
    }
    return left;
}

test "node size check" {
    try std.testing.expect(@sizeOf(Node(u8, 4096)) <= 4096);
    try std.testing.expect(@sizeOf(Node(u16, 4096)) <= 4096);
    try std.testing.expect(@sizeOf(Node(u32, 4096)) <= 4096);
    try std.testing.expect(@sizeOf(Node(u64, 4096)) <= 4096);

    try std.testing.expect(@sizeOf(Node(i8, 4096)) <= 4096);
    try std.testing.expect(@sizeOf(Node(i16, 4096)) <= 4096);
    try std.testing.expect(@sizeOf(Node(i32, 4096)) <= 4096);
    try std.testing.expect(@sizeOf(Node(i64, 4096)) <= 4096);

    try std.testing.expect(@sizeOf(Node(f32, 4096)) <= 4096);
    try std.testing.expect(@sizeOf(Node(f64, 4096)) <= 4096);
}

test "creating new row fails when key is too long" {
    const key: [Row.MAX_KEY_LEN + 1]u8 = undefined;
    const result = Row.new(0x0102BEEF, &key, "world");
    try std.testing.expectError(Row.Error.KeyTooLong, result);
}

test "creating new row fails when value is too long" {
    const val: [Row.MAX_VAL_LEN + 1]u8 = undefined;
    const result = Row.new(0x0102BEEF, "hello", &val);
    try std.testing.expectError(Row.Error.ValueTooLong, result);
}

test "table insert should update rows count" {
    const filepath = try squeal_assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

    var pager = try TestTable.Pager.init(std.testing.allocator, filepath);
    defer pager.deinit();

    var table = try TestTable.init(&pager);
    defer table.deinit() catch unreachable;

    const rows = [_]Row{
        try Row.new(0, "hello_0", "world_0"),
        try Row.new(1, "hello_1", "world_1"),
        try Row.new(2, "hello_2", "world_2"),
        try Row.new(3, "hello_3", "world_3"),
        try Row.new(4, "hello_4", "world_4"),
    };
    for (&rows) |*row| try table.insert(row);

    const num_rows = try table.count();
    try std.testing.expectEqual(rows.len, num_rows);
}

test "table select should should returns all available rows" {
    const filepath = try squeal_assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

    var pager = try TestTable.Pager.init(std.testing.allocator, filepath);
    defer pager.deinit();

    var table = try TestTable.init(&pager);
    defer table.deinit() catch unreachable;

    var i: u64 = 0;
    while (i < TestTable.TreeLeaf.MAX_CELLS) : (i += 1) {
        const row = try Row.new(i, "hello", "world");
        try table.insert(&row);
    }

    const rows = try table.select(std.testing.allocator);
    defer std.testing.allocator.free(rows);

    for (rows, 0..) |row, row_num| {
        try std.testing.expectEqual(@as(u64, @intCast(row_num)), row.id);
        try std.testing.expectEqualStrings("hello", row.key_buf[0..row.key_len]);
        try std.testing.expectEqualStrings("world", row.val_buf[0..row.val_len]);
    }
}

test "table persists between different runs" {
    const filepath = try squeal_assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

    var expected: [255]Row = undefined;
    for (&expected, 0..) |*row, row_num| row.* = try Row.new(row_num, "hello", "world");

    {
        var pager = try TestTable.Pager.init(std.testing.allocator, filepath);
        defer pager.deinit();

        var table = try TestTable.init(&pager);
        defer table.deinit() catch unreachable;

        for (&expected) |*row| try table.insert(row);
    }
    {
        var pager = try TestTable.Pager.init(std.testing.allocator, filepath);
        defer pager.deinit();

        var table = try TestTable.init(&pager);
        defer table.deinit() catch unreachable;

        const rows = try table.select(std.testing.allocator);
        defer std.testing.allocator.free(rows);

        try std.testing.expectEqualSlices(Row, &expected, rows);
    }
}
