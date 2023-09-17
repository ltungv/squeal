const std = @import("std");
const squeal_cli = @import("cli.zig");
const squeal_parse = @import("parse.zig");
const squeal_table = @import("table.zig");

pub const Vm = struct {
    allocator: std.mem.Allocator,
    stream: *const squeal_cli.Stream,
    table: squeal_table.Table,

    const Error = squeal_cli.Stream.ReadError || squeal_cli.Stream.WriteError || squeal_table.Table.Error;

    pub fn init(allocator: std.mem.Allocator, stream: *const squeal_cli.Stream, path: []const u8) Error!@This() {
        return .{
            .allocator = allocator,
            .stream = stream,
            .table = try squeal_table.Table.init(allocator, path),
        };
    }

    pub fn deinit(this: *@This()) void {
        this.table.deinit();
    }

    pub fn run(this: *@This()) Error!void {
        var finished = false;
        while (!finished) {
            try this.stream.print("db > ");

            var line_buf: [squeal_cli.MAX_LINE_BUFFER_SIZE]u8 = undefined;
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
        if (page.header.is_leaf) {
            const leaf = &page.body.leaf;
            try this.printIndentation(indentation);
            try this.stream.printf("- leaf (size {d})\n", .{leaf.num_cells});

            var cell_num: u32 = 0;
            while (cell_num < leaf.num_cells) : (cell_num += 1) {
                try this.printIndentation(indentation + 1);
                try this.stream.printf("  - {d}\n", .{leaf.cells[cell_num].key});
            }
        } else {
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
        }
    }
};
