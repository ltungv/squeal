const std = @import("std");
const testing = std.testing;
const tests = @import("tests.zig");

const Pager = @import("pager.zig").Pager;
const LeafNode = @import("pager.zig").LeafNode;

const Table = @import("table.zig").Table;
const Row = @import("table.zig").Row;

test "vm run .exit" {
    const filepath = try tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var input = std.ArrayList(u8).init(testing.allocator);
    defer input.deinit();
    var expected = std.ArrayList(u8).init(testing.allocator);
    defer expected.deinit();

    try input.appendSlice(".exit\n");
    try expected.appendSlice("db > ");

    try tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}

test "vm run empty line" {
    const filepath = try tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var input = std.ArrayList(u8).init(testing.allocator);
    defer input.deinit();
    var expected = std.ArrayList(u8).init(testing.allocator);
    defer expected.deinit();

    try input.appendSlice("\n");
    try input.appendSlice(".exit\n");
    try expected.appendSlice("db > error.UnrecognizedCommand\n");
    try expected.appendSlice("db > ");

    try tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}

test "vm run single insert then select" {
    const filepath = try tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var input = std.ArrayList(u8).init(testing.allocator);
    defer input.deinit();
    var expected = std.ArrayList(u8).init(testing.allocator);
    defer expected.deinit();

    try input.appendSlice("insert 1 'key' 'value'\n");
    try input.appendSlice("select\n");
    try input.appendSlice(".exit\n");
    try expected.appendSlice("db > Executed.\n");
    try expected.appendSlice("db > (1, key, value)\n");
    try expected.appendSlice("Executed.\n");
    try expected.appendSlice("db > ");

    try tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}

test "vm run max key and value size insert" {
    const filepath = try tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var input = std.ArrayList(u8).init(testing.allocator);
    defer input.deinit();
    var expected = std.ArrayList(u8).init(testing.allocator);
    defer expected.deinit();

    var max_size_key: [Row.MAX_KEY_LEN]u8 = undefined;
    std.mem.set(u8, &max_size_key, 'a');
    var max_size_val: [Row.MAX_VAL_LEN]u8 = undefined;
    std.mem.set(u8, &max_size_val, 'a');
    const insert = try std.fmt.allocPrint(testing.allocator, "insert 1 '{s}' '{s}'\n", .{ max_size_key, max_size_val });

    defer testing.allocator.free(insert);
    try input.appendSlice(insert);
    try input.appendSlice(".exit\n");
    try expected.appendSlice("db > Executed.\n");
    try expected.appendSlice("db > ");

    try tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}

test "vm run long key insert" {
    const filepath = try tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var input = std.ArrayList(u8).init(testing.allocator);
    defer input.deinit();
    var expected = std.ArrayList(u8).init(testing.allocator);
    defer expected.deinit();

    var long_key: [Row.MAX_KEY_LEN + 1]u8 = undefined;
    std.mem.set(u8, &long_key, 'a');
    const long_key_insert = try std.fmt.allocPrint(testing.allocator, "insert 1 '{s}' 'value'\n", .{long_key});
    defer testing.allocator.free(long_key_insert);

    try input.appendSlice(long_key_insert);
    try input.appendSlice(".exit\n");
    try expected.appendSlice("db > error.KeyTooLong\n");
    try expected.appendSlice("db > ");

    try tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}

test "vm run long value insert" {
    const filepath = try tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var input = std.ArrayList(u8).init(testing.allocator);
    defer input.deinit();
    var expected = std.ArrayList(u8).init(testing.allocator);
    defer expected.deinit();

    var long_value: [Row.MAX_VAL_LEN + 1]u8 = undefined;
    std.mem.set(u8, &long_value, 'a');
    const long_value_insert = try std.fmt.allocPrint(testing.allocator, "insert 1 'key' '{s}'\n", .{long_value});
    defer testing.allocator.free(long_value_insert);

    try input.appendSlice(long_value_insert);
    try input.appendSlice(".exit\n");
    try expected.appendSlice("db > error.ValueTooLong\n");
    try expected.appendSlice("db > ");

    try tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}

test "vm shows error when table is full" {
    const filepath = try tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var input = std.ArrayList(u8).init(testing.allocator);
    defer input.deinit();
    var expected = std.ArrayList(u8).init(testing.allocator);
    defer expected.deinit();

    var row: u32 = 0;
    while (row < LeafNode.MAX_CELLS + 1) : (row += 1) {
        const statement = try std.fmt.allocPrint(testing.allocator, "insert {d} 'key{d}' 'value{d}'\n", .{ row, row, row });
        defer testing.allocator.free(statement);
        try input.appendSlice(statement);

        if (row < LeafNode.MAX_CELLS) {
            try expected.appendSlice("db > Executed.\n");
        } else {
            try expected.appendSlice("db > error.TableFull\n");
        }
    }
    try input.appendSlice(".exit\n");
    try expected.appendSlice("db > ");

    try tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}

test "vm keeps data on reopen after closing" {
    const filepath = try tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);
    {
        var input = std.ArrayList(u8).init(testing.allocator);
        defer input.deinit();
        var expected = std.ArrayList(u8).init(testing.allocator);
        defer expected.deinit();

        var row: u32 = 0;
        while (row < 3) : (row += 1) {
            const statement = try std.fmt.allocPrint(testing.allocator, "insert {d} 'key{d}' 'value{d}'\n", .{ row, row, row });
            defer testing.allocator.free(statement);
            try input.appendSlice(statement);
            try expected.appendSlice("db > Executed.\n");
        }
        try input.appendSlice(".exit\n");
        try expected.appendSlice("db > ");

        try tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
    }
    {
        var input = std.ArrayList(u8).init(testing.allocator);
        defer input.deinit();
        var expected = std.ArrayList(u8).init(testing.allocator);
        defer expected.deinit();

        try input.appendSlice("select\n");
        var row: u32 = 0;
        while (row < 3) : (row += 1) {
            var returned_row: []u8 = undefined;
            if (row == 0) {
                returned_row = try std.fmt.allocPrint(testing.allocator, "db > ({d}, key{d}, value{d})\n", .{ row, row, row });
            } else {
                returned_row = try std.fmt.allocPrint(testing.allocator, "({d}, key{d}, value{d})\n", .{ row, row, row });
            }
            defer testing.allocator.free(returned_row);
            try expected.appendSlice(returned_row);
        }
        try expected.appendSlice("Executed.\n");
        try input.appendSlice(".exit\n");
        try expected.appendSlice("db > ");

        try tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
    }
}

test "vm allows printing one-node btree" {
    const filepath = try tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var input = std.ArrayList(u8).init(testing.allocator);
    defer input.deinit();
    var expected = std.ArrayList(u8).init(testing.allocator);
    defer expected.deinit();

    try input.appendSlice("insert 3 'key' 'value'\n");
    try input.appendSlice("insert 1 'key' 'value'\n");
    try input.appendSlice("insert 2 'key' 'value'\n");
    try input.appendSlice(".btree\n");

    try expected.appendSlice("db > Executed.\n");
    try expected.appendSlice("db > Executed.\n");
    try expected.appendSlice("db > Executed.\n");
    try expected.appendSlice("db > Tree:\n");
    try expected.appendSlice("leaf (size 3)\n");
    try expected.appendSlice("  - 0 : 1\n");
    try expected.appendSlice("  - 1 : 2\n");
    try expected.appendSlice("  - 2 : 3\n");

    try input.appendSlice(".exit\n");
    try expected.appendSlice("db > ");

    try tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}

test "vm allows printing contants" {
    const filepath = try tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var input = std.ArrayList(u8).init(testing.allocator);
    defer input.deinit();
    var expected = std.ArrayList(u8).init(testing.allocator);
    defer expected.deinit();

    try input.appendSlice(".constants\n");
    try input.appendSlice(".exit\n");

    try expected.appendSlice("db > Constants:\n");
    try expected.appendSlice("ROW_SIZE: 293\n");
    try expected.appendSlice("NODE_HEADER_SIZE: 5\n");
    try expected.appendSlice("NODE_TYPE_SIZE: 1\n");
    try expected.appendSlice("LEAF_NODE_SIZE: 3865\n");
    try expected.appendSlice("LEAF_NODE_CELL_SIZE: 297\n");
    try expected.appendSlice("LEAF_NODE_SPACE_FOR_CELLS: 4086\n");
    try expected.appendSlice("LEAF_NODE_MAX_CELLS: 13\n");
    try expected.appendSlice("INTERNAL_NODE_SIZE: 4088\n");
    try expected.appendSlice("INTERNAL_NODE_CELL_SIZE: 8\n");
    try expected.appendSlice("INTERNAL_NODE_SPACE_FOR_CELLS: 4082\n");
    try expected.appendSlice("INTERNAL_NODE_MAX_KEYS: 510\n");
    try expected.appendSlice("db > ");

    try tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}

test "vm shows error when inserting row with duplicate id" {
    const filepath = try tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var input = std.ArrayList(u8).init(testing.allocator);
    defer input.deinit();
    var expected = std.ArrayList(u8).init(testing.allocator);
    defer expected.deinit();

    try input.appendSlice("insert 0 'hello' 'world'\n");
    try input.appendSlice("insert 0 'hello' 'world'\n");
    try input.appendSlice(".exit\n");
    try expected.appendSlice("db > Executed.\n");
    try expected.appendSlice("db > error.DuplicateKey\n");
    try expected.appendSlice("db > ");

    try tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}
