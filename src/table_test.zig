const std = @import("std");
const testing = std.testing;
const tests = @import("tests.zig");

const LeafNode = @import("pager.zig").LeafNode;
const Pager = @import("pager.zig").Pager;

const Row = @import("table.zig").Row;
const Table = @import("table.zig").Table;

test "creating new row fails when key is too long" {
    const key: [Row.MAX_KEY_LEN + 1]u8 = undefined;
    const result = Row.new(0x0102BEEF, &key, "world");
    try testing.expectError(Row.Error.KeyTooLong, result);
}

test "creating new row fails when value is too long" {
    const val: [Row.MAX_VAL_LEN + 1]u8 = undefined;
    const result = Row.new(0x0102BEEF, "hello", &val);
    try testing.expectError(Row.Error.ValueTooLong, result);
}

test "serialize row" {
    const row = try Row.new(0x0102BEEF, "hello", "world");

    var buf: [Row.SERIALIZED_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try row.serialize(&ostream);

    var istream = std.io.fixedBufferStream(&buf);
    var reader = istream.reader();

    try testing.expectEqual(row.id, try reader.readInt(u32, .Little));
    try testing.expectEqual(row.key_len, try reader.readInt(u8, .Little));
    try testing.expectEqual(row.val_len, try reader.readInt(u8, .Little));
    try testing.expectEqual(row.key_buf, try reader.readBytesNoEof(Row.MAX_KEY_LEN));
    try testing.expectEqual(row.val_buf, try reader.readBytesNoEof(Row.MAX_VAL_LEN));
}

test "deserialize row" {
    const row = try Row.new(0x0102BEEF, "hello", "world");

    var buf: [Row.SERIALIZED_SIZE]u8 = undefined;
    var ostream = std.io.fixedBufferStream(&buf);
    try row.serialize(&ostream);

    var istream = std.io.fixedBufferStream(@as([]const u8, &buf));
    var row_new: Row = undefined;
    try row_new.deserialize(&istream);

    try testing.expectEqual(row, row_new);
}

test "table insert should update rows count" {
    const filepath = try tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var table = try Table.init(testing.allocator, filepath);
    defer table.deinit();

    const rows = [_]Row{
        try Row.new(0, "hello_0", "world_0"),
        try Row.new(1, "hello_1", "world_1"),
        try Row.new(2, "hello_2", "world_2"),
        try Row.new(3, "hello_3", "world_3"),
        try Row.new(4, "hello_4", "world_4"),
    };
    for (rows) |*row| {
        try table.insert(row);
    }
    var num_rows: u32 = 0;
    for (table.pager.pages[0..table.pager.pages_len]) |nullable_page| {
        if (nullable_page) |page| {
            switch (page.header.node_type) {
                .Leaf => num_rows += page.body.Leaf.num_cells,
                .Internal => {},
            }
        }
    }
    try testing.expectEqual(rows.len, num_rows);
}

test "table select should should returns all available rows" {
    const filepath = try tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var table = try Table.init(testing.allocator, filepath);
    defer table.deinit();

    var i: u32 = 0;
    while (i < LeafNode.MAX_CELLS) : (i += 1) {
        const row = try Row.new(i, "hello", "world");
        try table.insert(&row);
    }

    const rows = try table.select(testing.allocator);
    defer testing.allocator.free(rows);

    for (rows) |row, row_num| {
        try testing.expectEqual(@intCast(u32, row_num), row.id);
        try testing.expectEqualStrings("hello", row.key_buf[0..row.key_len]);
        try testing.expectEqualStrings("world", row.val_buf[0..row.val_len]);
    }
}

test "table persists between different runs" {
    const filepath = try tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var expected: [LeafNode.MAX_CELLS]Row = undefined;
    for (expected) |*row, row_num| {
        row.* = try Row.new(@intCast(u32, row_num), "hello", "world");
    }

    {
        var table = try Table.init(testing.allocator, filepath);
        defer table.deinit();

        for (expected) |*row| {
            try table.insert(row);
        }
    }
    {
        var table = try Table.init(testing.allocator, filepath);
        defer table.deinit();

        const rows = try table.select(testing.allocator);
        defer testing.allocator.free(rows);

        try testing.expectEqualSlices(Row, &expected, rows);
    }
}
