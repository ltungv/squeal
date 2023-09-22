const std = @import("std");
const testing = std.testing;
const squeal_tests = @import("tests.zig");
const squeal_pager = @import("pager.zig");
const squeal_table = @import("table.zig");

const Row = squeal_table.Row;
const Table = squeal_table.Table(Row, 4096, 64);

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

test "table insert should update rows count" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var pager = try Table.Pager.init(testing.allocator, filepath);
    defer pager.deinit();
    var table = try Table.init(&pager);
    defer table.deinit() catch unreachable;

    const rows = [_]Row{
        try Row.new(0, "hello_0", "world_0"),
        try Row.new(1, "hello_1", "world_1"),
        try Row.new(2, "hello_2", "world_2"),
        try Row.new(3, "hello_3", "world_3"),
        try Row.new(4, "hello_4", "world_4"),
    };
    for (&rows) |*row| {
        try table.insert(row);
    }
    var num_rows: u64 = 0;
    var cursor = try table.head();
    while (!cursor.end) {
        num_rows += 1;
        try cursor.advance();
    }
    try testing.expectEqual(rows.len, num_rows);
}

test "table select should should returns all available rows" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var pager = try Table.Pager.init(testing.allocator, filepath);
    defer pager.deinit();
    var table = try Table.init(&pager);
    defer table.deinit() catch unreachable;

    var i: u64 = 0;
    while (i < Table.NodeLeaf.MAX_CELLS) : (i += 1) {
        const row = try Row.new(i, "hello", "world");
        try table.insert(&row);
    }

    const rows = try table.select(testing.allocator);
    defer testing.allocator.free(rows);

    for (rows, 0..) |row, row_num| {
        try testing.expectEqual(@as(u64, @intCast(row_num)), row.id);
        try testing.expectEqualStrings("hello", row.key_buf[0..row.key_len]);
        try testing.expectEqualStrings("world", row.val_buf[0..row.val_len]);
    }
}

test "table persists between different runs" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);
    var expected: [255]Row = undefined;
    for (&expected, 0..) |*row, row_num| {
        row.* = try Row.new(row_num, "hello", "world");
    }
    {
        var pager = try Table.Pager.init(testing.allocator, filepath);
        defer pager.deinit();
        var table = try Table.init(&pager);
        defer table.deinit() catch unreachable;
        for (&expected) |*row| {
            try table.insert(row);
        }
    }
    {
        var pager = try Table.Pager.init(testing.allocator, filepath);
        defer pager.deinit();
        var table = try Table.init(&pager);
        defer table.deinit() catch unreachable;
        const rows = try table.select(testing.allocator);
        defer testing.allocator.free(rows);
        try testing.expectEqualSlices(Row, &expected, rows);
    }
}
