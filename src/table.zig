const std = @import("std");
const errors = @import("errors.zig");
const meta = @import("meta.zig");

const Cell = @import("pager.zig").Cell;
const Pager = @import("pager.zig").Pager;
const LeafNode = @import("pager.zig").LeafNode;

pub const Table = struct {
    pager: Pager,
    root_page: usize,

    pub const Error = error{
        TableFull,
        DuplicateKey,
    } || Pager.Error || Cursor.Error || Row.Error;

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, path: []const u8) Error!Self {
        var pager = try Pager.init(allocator, path);
        if (pager.pages_len == 0) {
            const leaf = LeafNode.new();
            var root = try pager.getPage(0);
            root.body = .{ .Leaf = leaf };
        }
        return Table{
            .pager = pager,
            .root_page = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        var page_num: usize = 0;
        while (page_num < self.pager.pages_len) : (page_num += 1) {
            if (self.pager.pages[page_num]) |page| {
                self.pager.flush(page_num) catch |err| {
                    std.log.err("Failed to flush page {d}: {!}", .{ page_num, err });
                };
                self.pager.allocator.destroy(page);
                self.pager.pages[page_num] = null;
            }
        }
        self.pager.deinit();
    }

    pub fn insert(self: *Self, row: *const Row) Error!void {
        const page = try self.pager.getPage(self.root_page);
        const num_cells = page.body.Leaf.num_cells;
        if (num_cells >= LeafNode.MAX_CELLS) return Error.TableFull;

        const key_to_insert = row.id;
        var cursor = try self.find(key_to_insert);

        if (cursor.cell < num_cells) {
            if (page.body.Leaf.cells[cursor.cell].key == key_to_insert) {
                return Error.DuplicateKey;
            }
        }

        try cursor.leafInsert(row.id, row);
    }

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

    pub fn head(self: *Self) Error!Cursor {
        const page = try self.pager.getPage(self.root_page);
        return .{
            .table = self,
            .page = self.root_page,
            .cell = 0,
            .end = page.body.Leaf.num_cells == 0,
        };
    }

    fn find(self: *Self, key: u32) Error!Cursor {
        const root_page = try self.pager.getPage(self.root_page);
        switch (root_page.body) {
            .Leaf => |leaf| {
                const num_cells = leaf.num_cells;
                var left: usize = 0;
                var right = @as(usize, num_cells);
                while (left < right) {
                    const index = (left + right) / 2;
                    const cell = leaf.cells[index];
                    if (key == cell.key) {
                        return .{
                            .table = self,
                            .page = self.root_page,
                            .cell = index,
                            .end = index + 1 >= num_cells,
                        };
                    }
                    if (key < cell.key) {
                        right = index;
                    } else {
                        left = index + 1;
                    }
                }
                return .{
                    .table = self,
                    .page = self.root_page,
                    .cell = left,
                    .end = left + 1 >= num_cells,
                };
            },
            // TODO: implement multi-level key finding.
            .Internal => unreachable,
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
    page: usize,
    cell: usize,
    end: bool,

    pub const Error = Pager.Error;

    const Self = @This();

    pub fn value(self: *const Self) Error!*Row {
        const page = try self.table.pager.getPage(self.page);
        return &page.body.Leaf.cells[self.cell].val;
    }

    pub fn value_view(self: *const Self) Error!*const Row {
        return self.value();
    }

    pub fn advance(self: *Self) Error!void {
        const page = try self.table.pager.getPage(self.page);
        self.cell += 1;
        self.end = self.cell >= page.body.Leaf.num_cells;
    }

    pub fn leafInsert(self: *Self, key: u32, val: *const Row) Error!void {
        const page = try self.table.pager.getPage(self.page);
        const num_cells = &page.body.Leaf.num_cells;
        if (num_cells.* >= LeafNode.MAX_CELLS) {
            return Error.OutOfBound;
        }

        var cells = &page.body.Leaf.cells;
        if (self.cell < num_cells.*) {
            var cell_num = num_cells.*;
            while (cell_num > self.cell) : (cell_num -= 1) {
                cells[cell_num] = cells[cell_num - 1];
            }
        }

        num_cells.* += 1;
        cells[self.cell] = .{ .key = key, .val = val.* };
    }
};
