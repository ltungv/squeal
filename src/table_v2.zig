const std = @import("std");
const squeal_assert = @import("assert.zig");
const squeal_lru = @import("lru.zig");
const squeal_table = @import("table.zig");

fn LruCache(comptime K: type, comptime V: type) type {
    return struct {
        allocator: std.mem.Allocator,
        capacity: usize,
        entries: HashMap,
        recency: TailQueue,

        const HashMap = std.AutoHashMap(K, *TailQueue.Node);

        const TailQueue = std.TailQueue(Entry);

        const Entry = struct { key: K, value: V };

        const Error = std.mem.Allocator.Error;

        fn init(allocator: std.mem.Allocator, capacity: usize) @This() {
            std.debug.assert(capacity > 0);
            return .{
                .allocator = allocator,
                .capacity = capacity,
                .entries = HashMap.init(allocator),
                .recency = TailQueue{},
            };
        }

        fn deinit(this: *@This()) void {
            this.entries.deinit();
            while (this.recency.pop()) |node| {
                this.allocator.destroy(node);
            }
        }

        fn get(this: *@This(), key: K) ?V {
            const node = this.entries.get(key) orelse {
                return null;
            };
            this.makeRecent(node);
            return node.data.value;
        }

        fn set(this: *@This(), key: K, value: V) Error!?Entry {
            const entry = try this.entries.getOrPut(key);
            if (entry.found_existing) {
                const node = entry.value_ptr.*;
                const replaced = node.data;
                node.data.value = value;
                this.makeRecent(node);
                return replaced;
            }
            entry.value_ptr.* = try this.allocator.create(TailQueue.Node);
            entry.value_ptr.*.data.key = key;
            entry.value_ptr.*.data.value = value;
            this.recency.prepend(entry.value_ptr.*);
            if (this.entries.count() > this.capacity) {
                const node = this.recency.pop().?;
                const invalidated = node.data;
                std.debug.assert(this.entries.remove(invalidated.key));
                this.allocator.destroy(node);
                return invalidated;
            }
            return null;
        }

        fn makeRecent(this: *@This(), node: *TailQueue.Node) void {
            this.recency.remove(node);
            this.recency.prepend(node);
        }
    };
}

const Pager = struct {
    allocator: std.mem.Allocator,
    cache: Cache,
    file: std.fs.File,
    file_size: u64,
    page_size: usize,
    page_count: u32,

    const Cache = LruCache(u32, []u8);

    const Error = error{ Corrupted, EndOfStream, FileSystem, NotSupported, NullPage, OutOfBound } ||
        std.fs.File.OpenError ||
        std.fs.File.PReadError ||
        std.fs.File.PWriteError ||
        std.mem.Allocator.Error ||
        std.process.GetCwdError;

    fn init(allocator: std.mem.Allocator, path: []const u8, page_size: usize, cache_size: usize) Error!Pager {
        std.debug.assert(page_size > 0);
        const cache = Cache.init(allocator, cache_size);
        const file = try std.fs.cwd().createFile(path, .{
            .read = true,
            .truncate = false,
            .exclusive = false,
        });
        const file_stat = try file.stat();
        const page_count = file_stat.size / page_size;
        if (page_size * page_count != file_stat.size or page_count > std.math.maxInt(u32)) {
            return Error.Corrupted;
        }
        return .{
            .allocator = allocator,
            .cache = cache,
            .file = file,
            .file_size = file_stat.size,
            .page_size = page_size,
            .page_count = @intCast(page_count),
        };
    }

    fn deinit(this: *Pager) void {
        var cache_node_iterator = this.cache.entries.valueIterator();
        while (cache_node_iterator.next()) |node| {
            this.allocator.free(node.*.data.value);
        }
        this.cache.deinit();
        this.file.close();
    }

    fn get(this: *Pager, id: u32) Error![]u8 {
        if (this.cache.get(id)) |page| {
            return page;
        }
        const page = try this.allocator.alloc(u8, this.page_size);
        if (id < this.file_size / this.page_size) {
            try this.readPage(id, page);
        }
        if (try this.cache.set(id, page)) |entry| {
            try this.writePage(entry.key, entry.value);
            this.allocator.free(entry.value);
        }
        this.page_count = @max(this.page_count, id + 1);
        return page;
    }

    fn flush(this: *Pager, id: u32) Error!void {
        const page = this.cache.get(id) orelse {
            return Error.NullPage;
        };
        try this.writePage(id, page);
    }

    fn flushAll(this: *Pager) Error!void {
        var id: u32 = 0;
        while (id < this.page_count) : (id += 1) {
            this.flush(id) catch |err| {
                if (err != error.NullPage) {
                    return err;
                }
            };
        }
    }

    fn getFreePage(this: *const Pager) u32 {
        return this.page_count;
    }

    fn readPage(this: *const Pager, id: u32, page: []u8) Error!void {
        const bytes = try this.file.preadAll(page, id * this.page_size);
        std.debug.assert(bytes == this.page_size);
    }

    fn writePage(this: *Pager, id: u32, page: []const u8) Error!void {
        std.debug.assert(page.len == this.page_size);
        try this.file.pwriteAll(page, id * this.page_size);
        this.file_size = @max(this.file_size, (id + 1) * this.page_size);
    }
};

// Node types
const LEAF_NODE_TYPE = 0;
const INNER_NODE_TYPE = 1;

// Common header
const NODE_TYPE_OFFSET = 0;
const NODE_TYPE_SIZE = @sizeOf(u8);
const NODE_HEADER_SIZE = NODE_TYPE_SIZE;

// Leaf node header
const LEAF_NODE_RIGHT_POINTER_OFFSET = NODE_HEADER_SIZE;
const LEAF_NODE_RIGHT_POINTER_SIZE = @sizeOf(u32);
const LEAF_NODE_CELL_COUNT_OFFSET = LEAF_NODE_RIGHT_POINTER_OFFSET + LEAF_NODE_RIGHT_POINTER_SIZE;
const LEAF_NODE_CELL_COUNT_SIZE = @sizeOf(u32);
const LEAF_NODE_HEADER_SIZE = NODE_HEADER_SIZE + LEAF_NODE_RIGHT_POINTER_SIZE + LEAF_NODE_CELL_COUNT_SIZE;

// Inner node header
const INNER_NODE_RIGHT_POINTER_OFFSET = NODE_HEADER_SIZE;
const INNER_NODE_RIGHT_POINTER_SIZE = @sizeOf(u32);
const INNER_NODE_CELL_COUNT_OFFSET = INNER_NODE_RIGHT_POINTER_OFFSET + INNER_NODE_RIGHT_POINTER_SIZE;
const INNER_NODE_CELL_COUNT_SIZE = @sizeOf(u32);
const INNER_NODE_HEADER_SIZE = NODE_HEADER_SIZE + INNER_NODE_RIGHT_POINTER_SIZE + INNER_NODE_CELL_COUNT_SIZE;

// Cell
const CELL_KEY_SIZE = @sizeOf(u64);
const CELL_POINTER_SIZE = @sizeOf(u32);

fn getNodeType(node: []const u8) u8 {
    return node[NODE_TYPE_OFFSET];
}

fn setNodeType(node: []u8, node_type: u8) void {
    node[NODE_TYPE_OFFSET] = node_type;
}

fn getLeafRightPointer(node: []const u8) u32 {
    std.debug.assert(getNodeType(node) == LEAF_NODE_TYPE);
    const buf = node[LEAF_NODE_RIGHT_POINTER_OFFSET .. LEAF_NODE_RIGHT_POINTER_OFFSET + LEAF_NODE_RIGHT_POINTER_SIZE];
    return std.mem.readInt(u32, buf, .little);
}

fn setLeafRightPointer(node: []u8, ptr: u32) void {
    std.debug.assert(getNodeType(node) == LEAF_NODE_TYPE);
    const buf = node[LEAF_NODE_RIGHT_POINTER_OFFSET .. LEAF_NODE_RIGHT_POINTER_OFFSET + LEAF_NODE_RIGHT_POINTER_SIZE];
    return std.mem.writeInt(u32, buf, ptr, .little);
}

fn getLeafCellCount(node: []const u8) u32 {
    std.debug.assert(getNodeType(node) == LEAF_NODE_TYPE);
    const buf = node[LEAF_NODE_CELL_COUNT_OFFSET .. LEAF_NODE_CELL_COUNT_OFFSET + LEAF_NODE_CELL_COUNT_SIZE];
    return std.mem.readInt(u32, buf, .little);
}

fn setLeafCellCount(node: []u8, count: u32) void {
    std.debug.assert(getNodeType(node) == LEAF_NODE_TYPE);
    const buf = node[LEAF_NODE_CELL_COUNT_OFFSET .. LEAF_NODE_CELL_COUNT_OFFSET + LEAF_NODE_CELL_COUNT_SIZE];
    std.mem.writeInt(u32, buf, count, .little);
}

fn getLeafCellKey(node: []const u8, cell_size: usize, id: u32) u64 {
    std.debug.assert(getNodeType(node) == LEAF_NODE_TYPE);
    const cell_offset = LEAF_NODE_HEADER_SIZE + cell_size * id;
    const cell = node[cell_offset .. cell_offset + cell_size];
    const buf = cell[0..CELL_KEY_SIZE];
    return std.mem.readInt(u64, buf, .little);
}

fn setLeafCellKey(node: []u8, cell_size: usize, id: u32, key: u64) void {
    std.debug.assert(getNodeType(node) == LEAF_NODE_TYPE);
    const cell_offset = LEAF_NODE_HEADER_SIZE + cell_size * id;
    const cell = node[cell_offset .. cell_offset + cell_size];
    const buf = cell[0..CELL_KEY_SIZE];
    std.mem.writeInt(u64, buf, key, .little);
}

fn getLeafCellValue(node: []const u8, value_size: usize, id: u32) []const u8 {
    std.debug.assert(getNodeType(node) == LEAF_NODE_TYPE);
    const cell_size = CELL_KEY_SIZE + value_size;
    const cell_offset = LEAF_NODE_HEADER_SIZE + cell_size * id;
    const cell = node[cell_offset .. cell_offset + cell_size];
    const buf = cell[CELL_KEY_SIZE .. CELL_KEY_SIZE + value_size];
    return buf;
}

fn setLeafCellValue(node: []u8, value_size: usize, id: u32, value: []const u8) void {
    std.debug.assert(getNodeType(node) == LEAF_NODE_TYPE);
    const cell_size = CELL_KEY_SIZE + value_size;
    const cell_offset = LEAF_NODE_HEADER_SIZE + cell_size * id;
    const cell = node[cell_offset .. cell_offset + cell_size];
    const buf = cell[CELL_KEY_SIZE .. CELL_KEY_SIZE + value_size];
    std.mem.copyBackwards(u8, buf, value);
}

fn initLeafNode(node: []u8) void {
    setNodeType(node, LEAF_NODE_TYPE);
    setLeafRightPointer(node, 0);
    setLeafCellCount(node, 0);
}

fn findLeaf(node: []const u8, cell_size: usize, key: u64) u32 {
    std.debug.assert(getNodeType(node) == LEAF_NODE_TYPE);
    var base: u32 = 0;
    var count: u32 = getLeafCellCount(node);
    while (count > 0) {
        const remainder = count % 2;
        count /= 2;
        const cell_key = getLeafCellKey(node, cell_size, base + count);
        base += @intFromBool(cell_key < key) * (count + remainder);
    }
    return base;
}

fn getInnerRightPointer(node: []const u8) u32 {
    std.debug.assert(getNodeType(node) == INNER_NODE_TYPE);
    const buf = node[INNER_NODE_RIGHT_POINTER_OFFSET .. INNER_NODE_RIGHT_POINTER_OFFSET + INNER_NODE_RIGHT_POINTER_SIZE];
    return std.mem.readInt(u32, buf, .little);
}

fn setInnerRightPointer(node: []u8, ptr: u32) void {
    std.debug.assert(getNodeType(node) == INNER_NODE_TYPE);
    const buf = node[INNER_NODE_RIGHT_POINTER_OFFSET .. INNER_NODE_RIGHT_POINTER_OFFSET + INNER_NODE_RIGHT_POINTER_SIZE];
    return std.mem.writeInt(u32, buf, ptr, .little);
}

fn getInnerCellCount(node: []const u8) u32 {
    std.debug.assert(getNodeType(node) == INNER_NODE_TYPE);
    const buf = node[INNER_NODE_CELL_COUNT_OFFSET .. INNER_NODE_CELL_COUNT_OFFSET + INNER_NODE_CELL_COUNT_SIZE];
    return std.mem.readInt(u32, buf, .little);
}

fn setInnerCellCount(node: []u8, count: u32) void {
    std.debug.assert(getNodeType(node) == INNER_NODE_TYPE);
    const buf = node[INNER_NODE_CELL_COUNT_OFFSET .. INNER_NODE_CELL_COUNT_OFFSET + INNER_NODE_CELL_COUNT_SIZE];
    std.mem.writeInt(u32, buf, count, .little);
}

fn getInnerCellKey(node: []const u8, id: u32) u64 {
    std.debug.assert(getNodeType(node) == INNER_NODE_TYPE);
    const cell_size = CELL_KEY_SIZE + CELL_POINTER_SIZE;
    const cell_offset = INNER_NODE_HEADER_SIZE + cell_size * id;
    const cell = node[cell_offset .. cell_offset + cell_size];
    const buf = cell[0..CELL_KEY_SIZE];
    return std.mem.readInt(u64, buf, .little);
}

fn setInnerCellKey(node: []u8, id: u32, key: u64) void {
    std.debug.assert(getNodeType(node) == INNER_NODE_TYPE);
    const cell_size = CELL_KEY_SIZE + CELL_POINTER_SIZE;
    const cell_offset = INNER_NODE_HEADER_SIZE + cell_size * id;
    const cell = node[cell_offset .. cell_offset + cell_size];
    const buf = cell[0..CELL_KEY_SIZE];
    std.mem.writeInt(u64, buf, key, .little);
}

fn getInnerCellPointer(node: []const u8, id: u32) u32 {
    std.debug.assert(getNodeType(node) == INNER_NODE_TYPE);
    const cell_size = CELL_KEY_SIZE + CELL_POINTER_SIZE;
    const cell_offset = INNER_NODE_HEADER_SIZE + cell_size * id;
    const cell = node[cell_offset .. cell_offset + cell_size];
    const buf = cell[CELL_KEY_SIZE .. CELL_KEY_SIZE + CELL_POINTER_SIZE];
    return std.mem.readInt(u32, buf, .little);
}

fn setInnerCellPointer(node: []u8, id: u32, ptr: u32) void {
    std.debug.assert(getNodeType(node) == INNER_NODE_TYPE);
    const cell_size = CELL_KEY_SIZE + CELL_POINTER_SIZE;
    const cell_offset = INNER_NODE_HEADER_SIZE + cell_size * id;
    const cell = node[cell_offset .. cell_offset + cell_size];
    const buf = cell[CELL_KEY_SIZE .. CELL_KEY_SIZE + CELL_POINTER_SIZE];
    std.mem.writeInt(u32, buf, ptr, .little);
}

fn initInnerNode(node: []u8) void {
    setNodeType(node, INNER_NODE_TYPE);
    setInnerRightPointer(node, 0);
    setInnerCellCount(node, 0);
}

fn findInner(node: []const u8, key: u64) u32 {
    std.debug.assert(getNodeType(node) == INNER_NODE_TYPE);
    var base: u32 = 0;
    var count: u32 = getInnerCellCount(node);
    while (count > 0) {
        const remainder = count % 2;
        count /= 2;
        const cell_key = getInnerCellKey(node, base + count);
        base += @intFromBool(cell_key < key) * (count + remainder);
    }
    return base;
}

const Table = struct {
    allocator: std.mem.Allocator,
    pager: Pager,
    height: usize,
    leaf_value_size: usize,
    leaf_cell_size: usize,
    max_leaf_cells: u32,
    max_inner_cells: u32,

    const Error = error{DuplicateKey} || Pager.Error;

    fn init(allocator: std.mem.Allocator, path: []const u8, leaf_value_size: usize, page_size: usize, page_count: usize) Error!@This() {
        const leaf_cell_size = CELL_KEY_SIZE + leaf_value_size;
        const max_leaf_cells = (page_size - LEAF_NODE_HEADER_SIZE) / leaf_cell_size;
        const max_inner_cells = (page_size - INNER_NODE_HEADER_SIZE) / (CELL_KEY_SIZE + CELL_POINTER_SIZE);
        var pager = try Pager.init(allocator, path, page_size, page_count);
        if (pager.page_count == 0) {
            initLeafNode(try pager.get(0));
        }
        return .{
            .allocator = allocator,
            .pager = pager,
            .height = 0,
            .leaf_value_size = leaf_value_size,
            .leaf_cell_size = leaf_cell_size,
            .max_leaf_cells = @intCast(max_leaf_cells),
            .max_inner_cells = @intCast(max_inner_cells),
        };
    }

    fn deinit(this: *@This()) void {
        this.pager.deinit();
    }

    fn find(this: *@This(), page: u32, key: u64) Error!Cursor {
        const node = try this.pager.get(page);
        switch (getNodeType(node)) {
            LEAF_NODE_TYPE => {
                const cell = findLeaf(node, this.leaf_cell_size, key);
                return .{
                    .table = this,
                    .page = page,
                    .cell = cell,
                    .done = cell >= getLeafCellCount(node),
                };
            },
            INNER_NODE_TYPE => {
                const cell = findInner(node, key);
                if (cell < getInnerCellCount(node)) {
                    return try this.find(getInnerCellPointer(node, cell), key);
                } else {
                    return try this.find(getInnerRightPointer(node), key);
                }
            },
            else => return Error.Corrupted,
        }
    }

    fn findWithAncestors(this: *@This(), page: u32, key: u64, ancestors: *std.ArrayList(u32)) Error!Cursor {
        const node = try this.pager.get(page);
        switch (getNodeType(node)) {
            LEAF_NODE_TYPE => {
                const cell = findLeaf(node, this.leaf_cell_size, key);
                return .{
                    .table = this,
                    .page = page,
                    .cell = cell,
                    .done = cell >= getLeafCellCount(node),
                };
            },
            INNER_NODE_TYPE => {
                try ancestors.append(page);
                const cell = findInner(node, key);
                if (cell < getInnerCellCount(node)) {
                    return try this.findWithAncestors(getInnerCellPointer(node, cell), key, ancestors);
                } else {
                    return try this.findWithAncestors(getInnerRightPointer(node), key, ancestors);
                }
            },
            else => return Error.Corrupted,
        }
    }

    fn insert(this: *@This(), key: u64, value: []const u8) Error!void {
        var ancestors = try std.ArrayList(u32).initCapacity(this.allocator, this.height);
        defer ancestors.deinit();
        const cursor = try this.findWithAncestors(0, key, &ancestors);
        const node = try this.pager.get(cursor.page);
        if (!cursor.done and key == getLeafCellKey(node, this.leaf_cell_size, cursor.cell)) {
            return Error.DuplicateKey;
        }
        try this.insertLeaf(node, cursor.cell, key, value, ancestors.items[0..ancestors.items.len]);
    }

    fn createRoot(this: *@This(), root: []u8, key: u64, rpage: u32, rnode: []u8) Error!void {
        const lpage = this.pager.getFreePage();
        const lnode = try this.pager.get(lpage);

        @memcpy(lnode, root);
        std.debug.assert(getNodeType(lnode) == getNodeType(rnode));

        initInnerNode(root);
        setInnerCellCount(root, 1);
        setInnerRightPointer(root, rpage);
        setInnerCellKey(root, 0, key);
        setInnerCellPointer(root, 0, lpage);
        this.height += 1;
    }

    fn insertLeaf(this: *@This(), node: []u8, cell: u32, key: u64, value: []const u8, ancestors: []u32) Error!void {
        std.debug.assert(getNodeType(node) == LEAF_NODE_TYPE);
        const cell_count = getLeafCellCount(node);
        if (cell_count >= this.max_leaf_cells) {
            try this.splitInsertLeaf(node, cell, key, value, ancestors);
            return;
        }

        const head_offset = LEAF_NODE_HEADER_SIZE + this.leaf_cell_size * cell;
        const tail_offset = LEAF_NODE_HEADER_SIZE + this.leaf_cell_size * cell_count;
        std.mem.copyBackwards(
            u8,
            node[head_offset..tail_offset],
            node[head_offset + this.leaf_cell_size .. tail_offset + this.leaf_cell_size],
        );

        setLeafCellKey(node, this.leaf_cell_size, cell, key);
        setLeafCellValue(node, this.leaf_value_size, cell, value);
        setLeafCellCount(node, cell_count + 1);
    }

    fn splitInsertLeaf(this: *@This(), lnode: []u8, cell: u32, key: u64, value: []const u8, ancestors: []u32) Error!void {
        std.debug.assert(getNodeType(lnode) == LEAF_NODE_TYPE);
        const rpage = this.pager.getFreePage();
        const rnode = try this.pager.get(rpage);
        initLeafNode(rnode);

        const splitted_rnode_cell_count = (this.max_leaf_cells + 1) / 2;
        const splitted_lnode_cell_count = (this.max_leaf_cells + 1) - splitted_rnode_cell_count;
        const lnode_old_max_key = try this.findTreeMaxKey(lnode);

        var idx = getLeafCellCount(lnode) + 1;
        while (idx > 0) : (idx -= 1) {
            const old_cell = idx - 1;
            const new_cell = old_cell % splitted_lnode_cell_count;
            var node: []u8 = undefined;
            if (old_cell >= splitted_lnode_cell_count) {
                node = rnode;
            } else {
                node = lnode;
            }
            if (old_cell < cell) {
                setLeafCellKey(node, this.leaf_cell_size, new_cell, getLeafCellKey(lnode, this.leaf_cell_size, old_cell));
                setLeafCellValue(node, this.leaf_value_size, new_cell, getLeafCellValue(lnode, this.leaf_value_size, old_cell));
            } else if (old_cell > cell) {
                setLeafCellKey(node, this.leaf_cell_size, new_cell, getLeafCellKey(lnode, this.leaf_cell_size, old_cell - 1));
                setLeafCellValue(node, this.leaf_value_size, new_cell, getLeafCellValue(lnode, this.leaf_value_size, old_cell - 1));
            } else {
                setLeafCellKey(node, this.leaf_cell_size, new_cell, key);
                setLeafCellValue(node, this.leaf_value_size, new_cell, value);
            }
        }

        setLeafCellCount(rnode, splitted_rnode_cell_count);
        setLeafCellCount(lnode, splitted_lnode_cell_count);
        setLeafRightPointer(rnode, getLeafRightPointer(lnode));
        setLeafRightPointer(lnode, rpage);

        const lnode_new_max_key = try this.findTreeMaxKey(lnode);
        if (ancestors.len == 0) {
            try this.createRoot(lnode, lnode_new_max_key, rpage, rnode);
            return;
        }

        const rnode_max_key = try this.findTreeMaxKey(rnode);
        const parent = try this.pager.get(ancestors[ancestors.len - 1]);
        try this.insertInner(parent, rnode_max_key, rpage, lnode_old_max_key, lnode_new_max_key, ancestors[0 .. ancestors.len - 1]);
    }

    fn insertInner(this: *@This(), node: []u8, key: u64, page: u32, lnode_old_max_key: u64, lnode_new_max_key: u64, ancestors: []u32) Error!void {
        std.debug.assert(getNodeType(node) == INNER_NODE_TYPE);
        const cell_count = getInnerCellCount(node);
        const lnode_old_max_key_cell = findInner(node, lnode_old_max_key);
        if (lnode_old_max_key_cell < cell_count) {
            setInnerCellKey(node, findInner(node, lnode_old_max_key), lnode_new_max_key);
        }
        if (cell_count >= this.max_inner_cells) {
            try this.splitInsertInner(node, key, page, ancestors);
            return;
        }

        const right_ptr = getInnerRightPointer(node);
        const right_most_node = try this.pager.get(right_ptr);
        const right_most_max_key = try this.findTreeMaxKey(right_most_node);
        if (key > right_most_max_key) {
            setInnerRightPointer(node, page);
            setInnerCellKey(node, cell_count, key);
            setInnerCellPointer(node, cell_count, right_ptr);
        } else {
            const cell = findInner(node, key);
            const head_offset = INNER_NODE_HEADER_SIZE + (CELL_KEY_SIZE + CELL_POINTER_SIZE) * cell;
            const tail_offset = INNER_NODE_HEADER_SIZE + (CELL_KEY_SIZE + CELL_POINTER_SIZE) * cell_count;
            std.mem.copyBackwards(
                u8,
                node[head_offset..tail_offset],
                node[head_offset + this.leaf_cell_size .. tail_offset + this.leaf_cell_size],
            );
            setInnerCellKey(node, cell, key);
            setInnerCellPointer(node, cell, page);
        }
        setInnerCellCount(node, cell_count + 1);
    }

    fn splitInsertInner(this: *@This(), lnode: []u8, key: u64, page: u32, ancestors: []u32) Error!void {
        std.debug.assert(getNodeType(lnode) == INNER_NODE_TYPE);
        const rpage = this.pager.getFreePage();
        const rnode = try this.pager.get(rpage);
        initInnerNode(rnode);

        const splitted_rnode_cell_count = (this.max_inner_cells + 1) / 2;
        const splitted_lnode_cell_count = (this.max_inner_cells + 1) - splitted_rnode_cell_count;
        const lnode_old_max_key = try this.findTreeMaxKey(lnode);
        const cell = findInner(lnode, key);

        var idx = getInnerCellCount(lnode) + 1;
        while (idx > 0) : (idx -= 1) {
            const old_cell = idx - 1;
            const new_cell = old_cell % splitted_lnode_cell_count;
            var node: []u8 = undefined;
            if (old_cell >= splitted_lnode_cell_count) {
                node = rnode;
            } else {
                node = lnode;
            }
            if (old_cell < cell) {
                setInnerCellKey(node, new_cell, getInnerCellKey(lnode, old_cell));
                setInnerCellPointer(node, new_cell, getInnerCellPointer(lnode, old_cell));
            } else if (old_cell > cell) {
                setInnerCellKey(node, new_cell, getInnerCellKey(lnode, old_cell - 1));
                setInnerCellPointer(node, new_cell, getInnerCellPointer(lnode, old_cell - 1));
            } else {
                setInnerCellKey(node, new_cell, key);
                setInnerCellPointer(node, new_cell, page);
            }
        }

        setInnerCellCount(rnode, splitted_rnode_cell_count);
        setInnerCellCount(lnode, splitted_lnode_cell_count - 1);

        if (key > lnode_old_max_key) {
            const rnode_cell = cell % splitted_lnode_cell_count;
            setInnerCellKey(rnode, rnode_cell, lnode_old_max_key);
            setInnerCellPointer(rnode, rnode_cell, getInnerRightPointer(lnode));
            setInnerRightPointer(rnode, page);
        } else {
            setInnerRightPointer(rnode, getInnerRightPointer(lnode));
        }
        setInnerRightPointer(lnode, rpage);

        const lnode_new_max_key = try this.findTreeMaxKey(lnode);
        if (ancestors.len == 0) {
            try this.createRoot(lnode, lnode_new_max_key, rpage, rnode);
            return;
        }

        const rnode_max_key = try this.findTreeMaxKey(rnode);
        const parent = try this.pager.get(ancestors[ancestors.len - 1]);
        try this.insertInner(parent, rnode_max_key, rpage, lnode_old_max_key, lnode_new_max_key, ancestors[0 .. ancestors.len - 1]);
    }

    fn findTreeMaxKey(this: *@This(), node: []u8) Error!u64 {
        switch (getNodeType(node)) {
            LEAF_NODE_TYPE => {
                const cell_count = getLeafCellCount(node);
                return getLeafCellKey(node, this.leaf_cell_size, cell_count - 1);
            },
            INNER_NODE_TYPE => {
                const right_ptr = getInnerRightPointer(node);
                const right_node = try this.pager.get(right_ptr);
                return this.findTreeMaxKey(right_node);
            },
            else => return Error.Corrupted,
        }
    }
};

const Cursor = struct {
    table: *Table,
    page: u32,
    cell: u32,
    done: bool,

    fn value(this: *const @This()) Table.Error![]const u8 {
        const node = try this.table.pager.get(this.page);
        return getLeafCellValue(node, this.table.leaf_value_size, this.cell);
    }

    fn advance(this: *@This()) Table.Error!void {
        const node = try this.table.pager.get(this.page);
        const cell_count = getLeafCellCount(node);
        this.cell += 1;
        if (this.cell >= cell_count) {
            this.page = getLeafRightPointer(node);
            this.cell = 0;
            this.done = this.page == 0;
        }
    }
};

const TestCache = LruCache(u32, u32);

const PagerTestState = struct {
    allocator: std.mem.Allocator,
    pages: [][]u8,

    fn init(allocator: std.mem.Allocator, page_size: usize, page_count: usize) !PagerTestState {
        var rng = std.rand.DefaultPrng.init(blk: {
            var seed: u64 = undefined;
            std.crypto.random.bytes(std.mem.asBytes(&seed));
            break :blk seed;
        });
        var pages = try allocator.alloc([]u8, page_count);
        for (0..page_count) |id| {
            pages[id] = try allocator.alloc(u8, page_size);
            rng.fill(pages[id]);
        }
        return .{ .allocator = allocator, .pages = pages };
    }

    fn deinit(this: *const PagerTestState) void {
        for (this.pages) |page| {
            this.allocator.free(page);
        }
        this.allocator.free(this.pages);
    }
};

const TableTestState = struct {
    allocator: std.mem.Allocator,
    entries: [][]u8,

    fn init(allocator: std.mem.Allocator, entry_count: usize, entry_size: usize) !TableTestState {
        var rng = std.rand.DefaultPrng.init(blk: {
            var seed: u64 = undefined;
            std.crypto.random.bytes(std.mem.asBytes(&seed));
            break :blk seed;
        });
        var entries = try allocator.alloc([]u8, entry_count);
        for (0..entry_count) |id| {
            entries[id] = try allocator.alloc(u8, entry_size);
            rng.fill(entries[id]);
        }
        return .{ .allocator = allocator, .entries = entries };
    }

    fn deinit(this: *const TableTestState) void {
        for (this.entries) |entry| {
            this.allocator.free(entry);
        }
        this.allocator.free(this.entries);
    }
};

test "LruCache.get returns entry added with LruCache.set" {
    var lru = TestCache.init(std.testing.allocator, 3);
    defer lru.deinit();

    try std.testing.expectEqual(null, lru.set(0, 69));
    try std.testing.expectEqual(null, lru.set(1, 420));
    try std.testing.expectEqual(null, lru.set(2, 666));
}

test "LruCache.set returns replaced entry on adding an existing key" {
    var lru = TestCache.init(std.testing.allocator, 3);
    defer lru.deinit();

    _ = try lru.set(0, 69);
    _ = try lru.set(1, 420);
    _ = try lru.set(2, 666);

    try std.testing.expectEqual(TestCache.Entry{ .key = 0, .value = 69 }, lru.set(0, 777));
    try std.testing.expectEqual(TestCache.Entry{ .key = 1, .value = 420 }, lru.set(1, 777));
    try std.testing.expectEqual(TestCache.Entry{ .key = 2, .value = 666 }, lru.set(2, 777));
}

test "LruCache.set returns invalidated entry on adding to a full cache" {
    var lru = TestCache.init(std.testing.allocator, 3);
    defer lru.deinit();

    _ = try lru.set(0, 69);
    _ = try lru.set(1, 420);
    _ = try lru.set(2, 666);

    try std.testing.expectEqual(TestCache.Entry{ .key = 0, .value = 69 }, lru.set(3, 69));
    try std.testing.expectEqual(TestCache.Entry{ .key = 1, .value = 420 }, lru.set(4, 420));
    try std.testing.expectEqual(TestCache.Entry{ .key = 2, .value = 666 }, lru.set(5, 666));
}

test "Pager.get returns a new page given new id" {
    const fpath = try squeal_assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(fpath);

    var pager = try Pager.init(std.testing.allocator, fpath, 4096, 64);
    defer pager.deinit();

    var page_ptrs = std.AutoHashMap([*]u8, void).init(std.testing.allocator);
    defer page_ptrs.deinit();

    for (0..64) |_| {
        const id = pager.getFreePage();
        const page = try pager.get(id);
        try page_ptrs.putNoClobber(page.ptr, undefined);
    }

    try std.testing.expectEqual(0, pager.file_size);
    try std.testing.expectEqual(64, pager.page_count);
}

test "Pager.flushAll writes all cached pages to disk" {
    const test_state = try PagerTestState.init(std.testing.allocator, 4096, 64);
    defer test_state.deinit();

    const fpath = try squeal_assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(fpath);

    {
        var pager = try Pager.init(std.testing.allocator, fpath, 4096, 64);
        defer pager.deinit();

        for (0.., test_state.pages) |id, expected_page| {
            const page = try pager.get(@intCast(id));
            @memcpy(page, expected_page);
        }
        try pager.flushAll();
    }

    var pager = try Pager.init(std.testing.allocator, fpath, 4096, 64);
    defer pager.deinit();

    try std.testing.expectEqual(64 * pager.page_size, pager.file_size);
    try std.testing.expectEqual(64, pager.page_count);

    for (0.., test_state.pages) |id, expected_page| {
        const page = try pager.get(@intCast(id));
        try std.testing.expectEqualSlices(u8, expected_page, page);
    }
}

test "Pager.set writes invalidated pages to disk" {
    const test_state = try PagerTestState.init(std.testing.allocator, 4096, 64);
    defer test_state.deinit();

    const fpath = try squeal_assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(fpath);

    {
        var pager = try Pager.init(std.testing.allocator, fpath, 4096, 1);
        defer pager.deinit();

        for (0.., test_state.pages) |id, expected_page| {
            const page = try pager.get(@intCast(id));
            @memcpy(page, expected_page);
        }
        try pager.flushAll();
    }

    var pager = try Pager.init(std.testing.allocator, fpath, 4096, 64);
    defer pager.deinit();

    try std.testing.expectEqual(64 * pager.page_size, pager.file_size);
    try std.testing.expectEqual(64, pager.page_count);

    for (0.., test_state.pages) |id, expected_page| {
        const page = try pager.get(@intCast(id));
        try std.testing.expectEqualSlices(u8, expected_page, page);
    }
}

test "Table.find returns entries added by Table.insert" {
    const fpath = try squeal_assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(fpath);

    var table = try Table.init(std.testing.allocator, fpath, 256, 1024, 512);
    defer table.deinit();

    var test_state = try TableTestState.init(std.testing.allocator, 10000, 256);
    defer test_state.deinit();

    for (0.., test_state.entries) |id, entry| {
        try table.insert(id, entry);
    }

    var cursor = try table.find(0, 0);
    for (test_state.entries) |entry| {
        try std.testing.expect(!cursor.done);
        const inserted_entry = try cursor.value();
        try std.testing.expectEqualSlices(u8, entry, inserted_entry);
        try cursor.advance();
    }
}
