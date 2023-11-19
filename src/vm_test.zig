const std = @import("std");
const testing = std.testing;
const squeal_tests = @import("tests.zig");
const squeal_table = @import("table.zig");

fn makeLines(comptime lines: []const []const u8) []const u8 {
    if (lines.len == 0) return "";
    comptime var result = lines[0];
    comptime for (lines[1..]) |line| {
        result = result ++ "\n" ++ line;
    };
    return result;
}

test "vm run .exit" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    const input = makeLines(&[_][]const u8{".exit"});
    const output = makeLines(&[_][]const u8{"db > "});
    try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, output, input);
}

test "vm run empty line" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    const input = makeLines(&[_][]const u8{
        "",
        ".exit",
    });
    const output = makeLines(&[_][]const u8{
        "db > error.UnrecognizedCommand",
        "db > ",
    });
    try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, output, input);
}

test "vm run single insert then select" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    const input = makeLines(&[_][]const u8{
        "insert 1 'key' 'value'",
        "select",
        ".exit",
    });
    const output = makeLines(&[_][]const u8{
        "db > Executed.",
        "db > (1, key, value)",
        "Executed.",
        "db > ",
    });
    try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, output, input);
}

test "vm run max key and value size insert" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    comptime var max_size_key: [squeal_table.Row.MAX_KEY_LEN]u8 = undefined;
    @memset(&max_size_key, 'a');
    comptime var max_size_val: [squeal_table.Row.MAX_VAL_LEN]u8 = undefined;
    @memset(&max_size_val, 'a');

    const input = makeLines(&[_][]const u8{
        std.fmt.comptimePrint("insert 1 '{s}' '{s}'", .{ max_size_key, max_size_val }),
        ".exit",
    });
    const output = makeLines(&[_][]const u8{
        "db > Executed.",
        "db > ",
    });
    try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, output, input);
}

test "vm run long key insert" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    comptime var long_key: [squeal_table.Row.MAX_KEY_LEN + 1]u8 = undefined;
    @memset(&long_key, 'a');

    const input = makeLines(&[_][]const u8{
        std.fmt.comptimePrint("insert 1 '{s}' 'value'", .{long_key}),
        ".exit",
    });
    const output = makeLines(&[_][]const u8{
        "db > error.KeyTooLong",
        "db > ",
    });
    try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, output, input);
}

test "vm run long value insert" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    comptime var long_value: [squeal_table.Row.MAX_VAL_LEN + 1]u8 = undefined;
    @memset(&long_value, 'a');

    const input = makeLines(&[_][]const u8{
        std.fmt.comptimePrint("insert 1 'key' '{s}'", .{long_value}),
        ".exit",
    });
    const output = makeLines(&[_][]const u8{
        "db > error.ValueTooLong",
        "db > ",
    });
    try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, output, input);
}

test "vm keeps data on reopen after closing" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);
    {
        const input = makeLines(&[_][]const u8{
            "insert 0 'key0' 'value0'",
            "insert 1 'key1' 'value1'",
            "insert 2 'key2' 'value2'",
            ".exit",
        });
        const output = makeLines(&[_][]const u8{
            "db > Executed.",
            "db > Executed.",
            "db > Executed.",
            "db > ",
        });
        try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, output, input);
    }
    {
        const input = makeLines(&[_][]const u8{
            "select",
            ".exit",
        });
        const output = makeLines(&[_][]const u8{
            "db > (0, key0, value0)",
            "(1, key1, value1)",
            "(2, key2, value2)",
            "Executed.",
            "db > ",
        });
        try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, output, input);
    }
}

test "vm allows printing one-node btree" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    const input = makeLines(&[_][]const u8{
        "insert 3 'key' 'value'",
        "insert 1 'key' 'value'",
        "insert 2 'key' 'value'",
        ".btree",
        ".exit",
    });
    const output = makeLines(&[_][]const u8{
        "db > Executed.",
        "db > Executed.",
        "db > Executed.",
        "db > Tree:",
        "- leaf (size 3)",
        "    - 1",
        "    - 2",
        "    - 3",
        "db > ",
    });
    try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, output, input);
}

test "vm shows error when inserting row with duplicate id" {
    const filepath = try squeal_tests.randomTemporaryFilePath(testing.allocator);
    defer testing.allocator.free(filepath);

    const input = makeLines(&[_][]const u8{
        "insert 0 'hello' 'world'",
        "insert 0 'hello' 'world'",
        ".exit",
    });
    const output = makeLines(&[_][]const u8{
        "db > Executed.",
        "db > error.DuplicateKey",
        "db > ",
    });
    try squeal_tests.expectVmOutputGivenInput(testing.allocator, filepath, output, input);
}
