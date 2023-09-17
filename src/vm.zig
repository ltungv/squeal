const std = @import("std");
const squeal_parse = @import("parse.zig");
const squeal_table = @import("table.zig");

pub const MAX_LINE_BUFFER_SIZE = 1024;

// The virtual machine that executes SQL statements.
pub const Vm = struct {
    allocator: std.mem.Allocator,
    stream: *const Stream,
    table: *squeal_table.Table,

    // VM's error.
    const Error = Stream.ReadError || Stream.WriteError || squeal_table.Table.Error;

    // Create a new VM.
    pub fn init(
        allocator: std.mem.Allocator,
        stream: *const Stream,
        table: *squeal_table.Table,
    ) Error!@This() {
        return .{
            .allocator = allocator,
            .stream = stream,
            .table = table,
        };
    }

    pub fn run(this: *@This()) Error!void {
        var finished = false;
        while (!finished) {
            try this.stream.print("db > ");

            var line_buf: [MAX_LINE_BUFFER_SIZE]u8 = undefined;
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
                .Constants => {},
                .Exit => return true,
            },
            .Query => |query| {
                switch (query) {
                    .Select => {
                        const rows = try this.table.select(this.allocator);
                        defer this.allocator.free(rows);

                        for (rows) |row| {
                            const key = row.key_buf[0..row.key_len];
                            const val = row.val_buf[0..row.val_len];
                            try this.stream.printf("({d}, {s}, {s})\n", .{ row.id, key, val });
                        }
                    },
                    .Insert => |q| {
                        try this.table.insert(&q.row);
                    },
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

    fn printTree(this: *@This(), page_num: u32, indentation: usize) Error!void {
        const page = try this.table.pager.get(page_num);
        switch (page.header.type) {
            .Leaf => {
                const leaf = &page.body.leaf;
                try this.printIndentation(indentation);
                try this.stream.printf("- leaf (size {d})\n", .{leaf.num_cells});
                var cell_num: u32 = 0;
                while (cell_num < leaf.num_cells) : (cell_num += 1) {
                    try this.printIndentation(indentation + 1);
                    try this.stream.printf("  - {d}\n", .{leaf.cells[cell_num].key});
                }
            },
            .Internal => {
                const internal = &page.body.internal;
                try this.printIndentation(indentation);
                try this.stream.printf("- internal (size {d})\n", .{internal.num_keys});
                var cell_num: u32 = 0;
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

    /// Create a new stream.
    pub fn new(istream: *std.io.StreamSource, ostream: *std.io.StreamSource) @This() {
        return .{ .reader = istream.reader(), .writer = ostream.writer() };
    }

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
