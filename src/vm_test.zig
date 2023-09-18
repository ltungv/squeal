const std = @import("std");
const testing = std.testing;
const squeal_tests = @import("tests.zig");
const squeal_table = @import("table.zig");

test "vm run .exit" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var input = std.ArrayList(u8).init(testing.allocator);
    defer input.deinit();
    var expected = std.ArrayList(u8).init(testing.allocator);
    defer expected.deinit();

    try input.appendSlice(".exit\n");
    try expected.appendSlice("db > ");

    try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}

test "vm run empty line" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var input = std.ArrayList(u8).init(testing.allocator);
    defer input.deinit();
    var expected = std.ArrayList(u8).init(testing.allocator);
    defer expected.deinit();

    try input.appendSlice("\n");
    try input.appendSlice(".exit\n");
    try expected.appendSlice("db > error.UnrecognizedCommand\n");
    try expected.appendSlice("db > ");

    try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}

test "vm run single insert then select" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
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

    try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}

test "vm run max key and value size insert" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var input = std.ArrayList(u8).init(testing.allocator);
    defer input.deinit();
    var expected = std.ArrayList(u8).init(testing.allocator);
    defer expected.deinit();

    var max_size_key: [squeal_table.Row.MAX_KEY_LEN]u8 = undefined;
    @memset(&max_size_key, 'a');
    var max_size_val: [squeal_table.Row.MAX_VAL_LEN]u8 = undefined;
    @memset(&max_size_val, 'a');

    const insert = try std.fmt.allocPrint(testing.allocator, "insert 1 '{s}' '{s}'\n", .{ max_size_key, max_size_val });
    defer testing.allocator.free(insert);
    try input.appendSlice(insert);
    try input.appendSlice(".exit\n");
    try expected.appendSlice("db > Executed.\n");
    try expected.appendSlice("db > ");

    try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}

test "vm run long key insert" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var input = std.ArrayList(u8).init(testing.allocator);
    defer input.deinit();
    var expected = std.ArrayList(u8).init(testing.allocator);
    defer expected.deinit();

    var long_key: [squeal_table.Row.MAX_KEY_LEN + 1]u8 = undefined;
    @memset(&long_key, 'a');
    const long_key_insert = try std.fmt.allocPrint(testing.allocator, "insert 1 '{s}' 'value'\n", .{long_key});
    defer testing.allocator.free(long_key_insert);

    try input.appendSlice(long_key_insert);
    try input.appendSlice(".exit\n");
    try expected.appendSlice("db > error.KeyTooLong\n");
    try expected.appendSlice("db > ");

    try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}

test "vm run long value insert" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    var input = std.ArrayList(u8).init(testing.allocator);
    defer input.deinit();
    var expected = std.ArrayList(u8).init(testing.allocator);
    defer expected.deinit();

    var long_value: [squeal_table.Row.MAX_VAL_LEN + 1]u8 = undefined;
    @memset(&long_value, 'a');
    const long_value_insert = try std.fmt.allocPrint(testing.allocator, "insert 1 'key' '{s}'\n", .{long_value});
    defer testing.allocator.free(long_value_insert);

    try input.appendSlice(long_value_insert);
    try input.appendSlice(".exit\n");
    try expected.appendSlice("db > error.ValueTooLong\n");
    try expected.appendSlice("db > ");

    try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}

test "vm keeps data on reopen after closing" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);
    {
        var input = std.ArrayList(u8).init(testing.allocator);
        defer input.deinit();
        var expected = std.ArrayList(u8).init(testing.allocator);
        defer expected.deinit();

        try input.appendSlice("insert 0 'key0' 'value0'\n");
        try input.appendSlice("insert 1 'key1' 'value1'\n");
        try input.appendSlice("insert 2 'key2' 'value2'\n");
        try input.appendSlice(".exit\n");
        try expected.appendSlice("db > Executed.\n");
        try expected.appendSlice("db > Executed.\n");
        try expected.appendSlice("db > Executed.\n");
        try expected.appendSlice("db > ");

        try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
    }
    {
        var input = std.ArrayList(u8).init(testing.allocator);
        defer input.deinit();
        var expected = std.ArrayList(u8).init(testing.allocator);
        defer expected.deinit();

        try input.appendSlice("select\n");
        try input.appendSlice(".exit\n");
        try expected.appendSlice("db > (0, key0, value0)\n");
        try expected.appendSlice("(1, key1, value1)\n");
        try expected.appendSlice("(2, key2, value2)\n");
        try expected.appendSlice("Executed.\n");
        try expected.appendSlice("db > ");

        try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
    }
}

test "vm allows printing one-node btree" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
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
    try expected.appendSlice("- leaf (size 3)\n");
    try expected.appendSlice("    - 1\n");
    try expected.appendSlice("    - 2\n");
    try expected.appendSlice("    - 3\n");

    try input.appendSlice(".exit\n");
    try expected.appendSlice("db > ");

    try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}

test "vm shows error when inserting row with duplicate id" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
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

    try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, expected.items, input.items);
}
