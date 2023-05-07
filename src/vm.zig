const std = @import("std");
const cli = @import("cli.zig");

const Parser = @import("parse.zig").Parser;
const Statement = @import("parse.zig").Statement;

const NodeType = @import("pager.zig").NodeType;
const NodeHeader = @import("pager.zig").NodeHeader;

const LeafNode = @import("pager.zig").LeafNode;
const LeafNodeCell = @import("pager.zig").LeafNodeCell;

const Row = @import("table.zig").Row;
const Table = @import("table.zig").Table;

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
                .BTree => try self.printTree(),
                .Constants => try self.printConstants(),
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

    fn printTree(self: *Self) Error!void {
        try self.stream.print("Tree:\n");
        const page = try self.table.pager.getPage(self.table.root_page);
        switch (page.body) {
            .Leaf => |leaf| {
                var cell_num: usize = 0;
                try self.stream.printf("leaf (size {d})\n", .{leaf.num_cells});
                while (cell_num < leaf.num_cells) : (cell_num += 1) {
                    try self.stream.printf("  - {d} : {d}\n", .{ cell_num, leaf.cells[cell_num].key });
                }
            },
            .Internal => |internal| {
                var cell_num: usize = 0;
                try self.stream.printf("internal (size {d})\n", .{internal.num_keys});
                while (cell_num < internal.num_keys) : (cell_num += 1) {
                    try self.stream.printf("  - {d} : {d}\n", .{ cell_num, internal.cells[cell_num].key });
                }
            },
        }
    }

    fn printConstants(self: *const Self) Error!void {
        try self.stream.print("Constants:\n");
        try self.stream.printf("ROW_SIZE: {d}\n", .{Row.SERIALIZED_SIZE});
        try self.stream.printf("NODE_HEADER_SIZE: {d}\n", .{NodeHeader.SERIALIZED_SIZE});
        try self.stream.printf("LEAF_NODE_SIZE: {d}\n", .{LeafNode.SERIALIZED_SIZE});
        try self.stream.printf("LEAF_NODE_CELL_SIZE: {d}\n", .{LeafNodeCell.SERIALIZED_SIZE});
        try self.stream.printf("LEAF_NODE_SPACE_FOR_CELLS: {d}\n", .{LeafNode.SPACE_FOR_CELLS});
        try self.stream.printf("LEAF_NODE_MAX_CELLS: {d}\n", .{LeafNode.MAX_CELLS});
    }
};
