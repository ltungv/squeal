const std = @import("std");
const cli = @import("cli.zig");
const libparse = @import("parse.zig");
const libtable = @import("table.zig");

const Statement = libparse.Statement;
const Parser = libparse.Parser;
const Table = libtable.Table;

pub const Vm = struct {
    allocator: std.mem.Allocator,
    stream: *const cli.Stream,
    table: Table,

    const Error = cli.Stream.ReadError || cli.Stream.WriteError || Table.Error;

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, stream: *const cli.Stream, path: []const u8) Error!Self {
        return .{
            .allocator = allocator,
            .stream = stream,
            .table = try Table.init(allocator, path),
        };
    }

    pub fn deinit(self: *Self) void {
        self.table.deinit();
    }

    pub fn run(self: *Self) Error!void {
        var finished = false;
        while (!finished) {
            try self.stream.print("db > ");

            var line_buf: [cli.MAX_LINE_BUFFER_SIZE]u8 = undefined;
            const line = self.stream.readln(&line_buf) catch |err| {
                try self.stream.eprint(err);
                continue;
            } orelse {
                continue;
            };

            var parser = Parser.new(line);
            const statement = parser.parse() catch |err| {
                try self.stream.eprint(err);
                continue;
            };

            finished = self.exec(&statement) catch |err| {
                try self.stream.eprint(err);
                continue;
            };
        }
    }

    fn exec(self: *Self, statement: *const Statement) Error!bool {
        switch (statement.*) {
            .Command => |command| switch (command) {
                .BTree => {
                    try self.stream.print("Tree:\n");
                    try self.printTree(self.table.root_page, 0);
                },
                .Constants => {},
                .Exit => return true,
            },
            .Query => |query| {
                switch (query) {
                    .Select => {
                        const rows = try self.table.select(self.allocator);
                        defer self.allocator.free(rows);

                        for (rows) |row| {
                            const key = row.key_buf[0..row.key_len];
                            const val = row.val_buf[0..row.val_len];
                            try self.stream.printf("({d}, {s}, {s})\n", .{ row.id, key, val });
                        }
                    },
                    .Insert => |q| {
                        try self.table.insert(&q.row);
                    },
                }
                try self.stream.print("Executed.\n");
            },
        }
        return false;
    }

    fn printIndentation(self: *Self, level: usize) Error!void {
        var i = level;
        while (i > 0) : (i -= 1) {
            try self.stream.print("  ");
        }
    }

    fn printTree(self: *Self, page_num: u32, indentation: usize) Error!void {
        const page = try self.table.pager.get(page_num);
        if (page.header.is_leaf) {
            const leaf = &page.body.leaf;
            try self.printIndentation(indentation);
            try self.stream.printf("- leaf (size {d})\n", .{leaf.num_cells});

            var cell_num: u32 = 0;
            while (cell_num < leaf.num_cells) : (cell_num += 1) {
                try self.printIndentation(indentation + 1);
                try self.stream.printf("  - {d}\n", .{leaf.cells[cell_num].key});
            }
        } else {
            const internal = &page.body.internal;
            try self.printIndentation(indentation);
            try self.stream.printf("- internal (size {d})\n", .{internal.num_keys});

            var cell_num: u32 = 0;
            while (cell_num < internal.num_keys) : (cell_num += 1) {
                const cell = internal.cells[cell_num];
                try self.printTree(cell.val, indentation + 1);
                try self.printIndentation(indentation + 1);
                try self.stream.printf("- key {d}\n", .{cell.key});
            }
            try self.printTree(internal.right_child, indentation + 1);
        }
    }
};
