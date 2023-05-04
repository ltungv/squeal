const std = @import("std");
const errors = @import("errors.zig");

const Pager = @import("pager.zig").Pager;

pub const Table = struct {
    pager: Pager,
    rows: usize,

    pub const ROWS_PER_PAGE = Pager.PAGE_SIZE / @sizeOf(Row);
    pub const MAX_ROWS = ROWS_PER_PAGE * Pager.MAX_PAGES;

    pub const Error = error{TableFull} || Pager.Error || Cursor.Error || Row.Error || errors.IoError;

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, path: []const u8) Error!Self {
        const pager = try Pager.init(allocator, path);
        const rows = pager.len / Pager.PAGE_SIZE * ROWS_PER_PAGE + (pager.len % Pager.PAGE_SIZE) / @sizeOf(Row);
        return Table{
            .pager = pager,
            .rows = rows,
        };
    }

    pub fn deinit(self: *Self) void {
        var page_num: usize = 0;
        const num_full_pages = self.rows / ROWS_PER_PAGE;
        while (page_num < num_full_pages) : (page_num += 1) {
            if (self.pager.cache[page_num]) |page| {
                self.pager.flush(page_num, Pager.PAGE_SIZE) catch |err| {
                    std.log.err("Failed to flush page {d}: {!}", .{ page_num, err });
                };
                self.pager.allocator.free(page);
                self.pager.cache[page_num] = null;
            }
        }
        const num_additional_rows = self.rows % ROWS_PER_PAGE;
        if (num_additional_rows > 0) {
            if (self.pager.cache[page_num]) |page| {
                self.pager.flush(page_num, num_additional_rows * @sizeOf(Row)) catch |err| {
                    std.log.err("Failed to flush page {d}: {!}", .{ page_num, err });
                };
                self.pager.allocator.free(page);
                self.pager.cache[page_num] = null;
            }
        }
        self.pager.deinit();
    }

    pub fn insert(self: *Self, row: *const Row) Error!void {
        if (self.rows >= MAX_ROWS) return Error.TableFull;
        const cursor = self.tail();
        const row_slot = try cursor.value();
        var stream = std.io.fixedBufferStream(row_slot);
        try row.serialize(&stream);
        self.rows += 1;
    }

    pub fn select(self: *Self, allocator: std.mem.Allocator) Error![]Row {
        var rows = try allocator.alloc(Row, self.rows);
        var cursor = self.head();
        while (!cursor.end) {
            const row_slot = try cursor.value_view();
            var stream = std.io.fixedBufferStream(row_slot);
            try rows[cursor.row].deserialize(&stream);
            cursor.advance();
        }
        return rows;
    }

    pub fn head(self: *Self) Cursor {
        return .{
            .table = self,
            .row = 0,
            .end = self.rows == 0,
        };
    }

    pub fn tail(self: *Self) Cursor {
        return .{
            .table = self,
            .row = self.rows,
            .end = true,
        };
    }
};

pub const Row = struct {
    id: i32,
    key_len: u8,
    val_len: u8,
    key_buf: [MAX_KEY_LEN]u8,
    val_buf: [MAX_VAL_LEN]u8,

    pub const MAX_KEY_LEN = 1 << 8 - 1;
    pub const MAX_VAL_LEN = 1 << 8 - 1;

    pub const Error = error{ KeyTooLong, ValueTooLong };

    const Self = @This();

    pub fn new(id: i32, key: []const u8, val: []const u8) Error!Self {
        if (key.len > MAX_KEY_LEN) return Error.KeyTooLong;
        if (val.len > MAX_VAL_LEN) return Error.ValueTooLong;
        var row = Row{
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

    pub fn serialize(self: *const Self, stream: *std.io.FixedBufferStream([]u8)) errors.IoError!void {
        var writer = stream.writer();
        try writer.writeInt(i32, self.id, .Little);
        try writer.writeInt(u8, self.key_len, .Little);
        try writer.writeInt(u8, self.val_len, .Little);
        try writer.writeAll(&self.key_buf);
        try writer.writeAll(&self.val_buf);
    }

    pub fn deserialize(self: *Self, stream: *std.io.FixedBufferStream([]const u8)) errors.IoError!void {
        var reader = stream.reader();
        self.id = try reader.readInt(i32, .Little);
        self.key_len = try reader.readInt(u8, .Little);
        self.val_len = try reader.readInt(u8, .Little);
        try reader.readNoEof(&self.key_buf);
        try reader.readNoEof(&self.val_buf);
    }
};

pub const Cursor = struct {
    table: *Table,
    row: usize,
    end: bool,

    pub const Error = Pager.Error;

    const Self = @This();

    pub fn value(self: *const Self) Error![]u8 {
        const page_num = self.row / Table.ROWS_PER_PAGE;
        const page = try self.table.pager.getPage(page_num);
        const row_offset = self.row % Table.ROWS_PER_PAGE;
        const byte_offset = row_offset * @sizeOf(Row);
        return page[byte_offset .. byte_offset + @sizeOf(Row)];
    }

    pub fn value_view(self: *const Self) Error![]const u8 {
        return self.value();
    }

    pub fn advance(self: *Self) void {
        self.row += 1;
        self.end = self.row >= self.table.rows;
    }
};
