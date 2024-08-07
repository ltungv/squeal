const std = @import("std");
const squeal_assert = @import("assert.zig");
const squeal_parse = @import("parse.zig");
const squeal_table = @import("table.zig");

pub const PAGE_SIZE = 32 * 1024;
pub const PAGE_COUNT = 1024 * 1024;
pub const Table = squeal_table.Table(squeal_table.Row, PAGE_SIZE, PAGE_COUNT);

// The virtual machine that executes SQL statements.
pub const Vm = struct {
    allocator: std.mem.Allocator,
    stream: *const Stream,
    table: *Table,

    /// VM's error.
    const Error = Stream.ReadError || Stream.WriteError || Table.Error;

    /// Create a new VM.
    pub fn init(
        allocator: std.mem.Allocator,
        stream: *const Stream,
        table: *Table,
    ) Error!@This() {
        return .{
            .allocator = allocator,
            .stream = stream,
            .table = table,
        };
    }

    /// Run the VM which repeatedly reads a line from the stream, parses it, and executes it.
    pub fn run(this: *@This()) Error!void {
        var finished = false;
        while (!finished) {
            try this.stream.print("db > ");

            var line_buf: [PAGE_SIZE]u8 = undefined;
            const line = this.stream.readln(&line_buf) catch |err| {
                try this.stream.eprint(err);
                continue;
            } orelse {
                continue;
            };

            var parser = squeal_parse.Parser.new(line);
            const statement = parser.parse() catch |err| {
                try this.stream.eprint(err);
                continue;
            };

            finished = this.exec(&statement) catch |err| {
                try this.stream.eprint(err);
                continue;
            };
        }
    }

    fn exec(this: *@This(), statement: *const squeal_parse.Statement) Error!bool {
        switch (statement.*) {
            .Command => |command| switch (command) {
                .BTree => {
                    try this.stream.print("Tree:\n");
                    try this.printTree(this.table.root_page, 0);
                },
                .Exit => return true,
            },
            .Query => |query| {
                switch (query) {
                    .Count => {
                        const count = try this.table.count();
                        try this.stream.printf("{}\n", .{count});
                    },
                    .Select => {
                        const rows = try this.table.select(this.allocator);
                        defer this.allocator.free(rows);
                        for (rows) |row| {
                            const key = row.key_buf[0..row.key_len];
                            const val = row.val_buf[0..row.val_len];
                            try this.stream.printf("({d}, {s}, {s})\n", .{ row.id, key, val });
                        }
                    },
                    .Insert => |q| try this.table.insert(&q.row),
                }
                try this.stream.print("Executed.\n");
            },
        }
        return false;
    }

    fn printIndentation(this: *@This(), level: usize) Error!void {
        var i = level;
        while (i > 0) : (i -= 1) {
            try this.stream.print("  ");
        }
    }

    fn printTree(this: *@This(), page_num: u64, indentation: usize) Error!void {
        const page = try this.table.pager.get(page_num);
        switch (page.header.type) {
            .Leaf => {
                const leaf = &page.body.leaf;
                try this.printIndentation(indentation);
                try this.stream.printf("- leaf (size {d})\n", .{leaf.num_cells});
                var cell_num: u64 = 0;
                while (cell_num < leaf.num_cells) : (cell_num += 1) {
                    try this.printIndentation(indentation + 1);
                    try this.stream.printf("  - {d}\n", .{leaf.cells[cell_num].key});
                }
            },
            .Internal => {
                const internal = &page.body.internal;
                try this.printIndentation(indentation);
                try this.stream.printf("- internal (size {d})\n", .{internal.num_keys});
                var cell_num: u64 = 0;
                while (cell_num < internal.num_keys) : (cell_num += 1) {
                    const cell = internal.cells[cell_num];
                    try this.printTree(cell.val, indentation + 1);
                    try this.printIndentation(indentation + 1);
                    try this.stream.printf("- key {d}\n", .{cell.key});
                }
                try this.printTree(internal.right_child, indentation + 1);
            },
        }
    }
};

/// A wrapper around a pair of StreamSource.Reader and StreamSource.Writer.
pub const Stream = struct {
    reader: std.io.StreamSource.Reader,
    writer: std.io.StreamSource.Writer,

    /// Error that occurs when writing to the stream.
    pub const WriteError = std.io.StreamSource.WriteError;

    /// Error that occurs when reading from the stream.
    pub const ReadError = error{ StreamTooLong, NoSpaceLeft } || std.io.StreamSource.ReadError;

    /// Print a simple string.
    pub fn print(this: *const @This(), comptime format: []const u8) WriteError!void {
        try this.writer.print(format, .{});
    }

    /// Print a formatted string.
    pub fn printf(this: *const @This(), comptime format: []const u8, args: anytype) WriteError!void {
        try this.writer.print(format, args);
    }

    /// Print an error in its default format.
    pub fn eprint(this: *const @This(), err: anyerror) WriteError!void {
        try this.writer.print("{!}\n", .{err});
    }

    /// Read a single line from the stream into an owned slice.
    pub fn readln(this: *const @This(), buf: []u8) ReadError!?[]u8 {
        return this.reader.readUntilDelimiterOrEof(buf, '\n');
    }
};

fn makeLines(comptime lines: []const []const u8) []const u8 {
    if (lines.len == 0) return "";
    comptime var result = lines[0];
    comptime for (lines[1..]) |line| {
        result = result ++ "\n" ++ line;
    };
    return result;
}

test "vm run .exit" {
    const filepath = try squeal_assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

    const input = makeLines(&[_][]const u8{".exit"});
    const output = makeLines(&[_][]const u8{"db > "});
    try squeal_assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
}

test "vm run empty line" {
    const filepath = try squeal_assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

    const input = makeLines(&[_][]const u8{
        "",
        ".exit",
    });
    const output = makeLines(&[_][]const u8{
        "db > error.UnrecognizedCommand",
        "db > ",
    });
    try squeal_assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
}

test "vm run single insert then select" {
    const filepath = try squeal_assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

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
    try squeal_assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
}

test "vm run max key and value size insert" {
    const filepath = try squeal_assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

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
    try squeal_assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
}

test "vm run long key insert" {
    const filepath = try squeal_assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

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
    try squeal_assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
}

test "vm run long value insert" {
    const filepath = try squeal_assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

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
    try squeal_assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
}

test "vm keeps data on reopen after closing" {
    const filepath = try squeal_assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);
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
        try squeal_assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
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
        try squeal_assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
    }
}

test "vm allows printing one-node btree" {
    const filepath = try squeal_assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

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
    try squeal_assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
}

test "vm shows error when inserting row with duplicate id" {
    const filepath = try squeal_assert.randomTemporaryFilePath(std.testing.allocator);
    defer std.testing.allocator.free(filepath);

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
    try squeal_assert.expectVmOutputGivenInput(std.testing.allocator, filepath, output, input);
}
